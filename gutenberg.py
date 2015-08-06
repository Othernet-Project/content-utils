#! /usr/bin/env python

import re, sys, gzip, json, tarfile
from os.path import isfile
from bs4 import BeautifulSoup
import xmltodict

"""
https://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.bz2
vague benchmarks suggest that the json version is 3x smaller and 30x faster to use

todo:

automatically download rdf
automatically re-download if stale

filter by media type (only books?  prefer html? prefer epub?)
    <dcterms:type> ... <rdf:value>
    <dcterms:hasFormat> ... <rdf:value>  (multiple of these)

generate filelist
rsync filelist
    though keep files that recently fell off of most-popular

convert new/updated files to zipball
extract metadata for bookshelf app

smells like there is a memory leak or something not being freed (1.5GB at exit)
nope, the structure takes up 1.3GB on a fresh load
metadata appears to be missing the "LoC Class" field

language field is sometimes wrong (dante's italian is all marked 'en')
"""

# these have been reported upstream
missing_titles = {
    "ebooks/997": "Divina Commedia di Dante: Inferno",
    "ebooks/998": "Divina Commedia di Dante: Purgatorio",
    "ebooks/999": "Divina Commedia di Dante: Paradiso",
    "ebooks/1000": "Divina Commedia di Dante",
    "ebooks/49344": "The Queen's Favourite",
}

def rdf_iterator(rdf_tar):
    "returns file object and xml text"
    skips = '0 999999'
    skips = set(skips.split())
    id_num = re.compile('pg([^.]*).rdf')
    for m in rdf_tar:
        if not m.name.endswith('.rdf'):
            continue
        name = id_num.search(m.name).group(1)
        if name in skips:
            continue
        xml = rdf_tar.extractfile(m).read()
        if sys.version_info >= (3, 0):
            xml = xml.decode()
        yield m, xml

def error(rdf):
    print(type(rdf))
    #print(rdf)
    print(json.dumps(rdf, indent=2))
    raise

def base_type(i):
    if issubclass(type(i), dict):
        return dict
    if issubclass(type(i), list):
        return list
    if issubclass(type(i), str):
        return str
    if issubclass(type(i), unicode):
        return str

def listify(stuff):
    if base_type(stuff) == list:
        return stuff
    return [stuff]

def rdf_val(rdf, recurse=False):
    # probably fragile, but covers a huge amount of crazy
    items = []
    if base_type(rdf) == str and not recurse:
        items.append(rdf)
    elif base_type(rdf) == list:
        for k in rdf:
            items.extend(rdf_val(k, recurse=True))
    elif base_type(rdf) == dict:
        for k,v in rdf.items():
            if k == '#text':
                items.append(v)
            elif k == 'rdf:value' and base_type(v) == str:
                items.append(v)
            else:
                items.extend(rdf_val(v, recurse=True))
    elif base_type(rdf) == str and recurse:
        pass
    else:
        error(rdf)
    return items

def metadata(rdf_files):
    for m, xml in rdf_iterator(rdf_files):
        j = xmltodict.parse(xml)
        # what about non-ebooks?  (nope, even the audio stuff is under ebook)
        j = j['rdf:RDF']['pgterms:ebook']
        base = j['@rdf:about']
        print(base)
        data = {}
        data['base_url'] = 'https://www.gutenberg.org/' + base
        data['id'] = base

        tag_map = {'dcterms:title': 'title',
                   'dcterms:rights': 'license',
                   'dcterms:publisher': 'publisher',
                   'dcterms:language': 'language',
                   'pgterms:downloads': 'downloads',
                   'pgterms:bookshelf': 'bookshelf',
                   'dcterms:subject': 'subjects',
                   'dcterms:type': 'media_type',
                   'dcterms:issued': 'release_date',
                  }
        quiet = 'bookshelf'.split()

        for t1,t2 in tag_map.items():
            if t1 not in j:
                if t2 not in quiet:
                    print('    warning: %s missing %s' % (base, t2))
                data[t2] = []
                continue
            try:
                data[t2] = rdf_val(j[t1])
            except:
                error(j)

        # clean up the simple cases
        no_nest = 'downloads release_date media_type title license publisher'.split()
        len_one = 'downloads media_type license publisher'.split()
        for k in no_nest:
            assert len(data[k]) <= 1
            if k in len_one:
                assert len(data[k]) == 1
            if len(data[k]) == 0:
                data[k] = None
            else:
                data[k] = data[k][0]

        data['downloads'] = int(data['downloads'])
        if data['license'] == 'None':
            data['license'] = None

        # correct missing info
        if data['title'] is None and base in missing_titles:
            data['title'] = missing_titles[base]

        # quite possibly the worst part
        creators = []
        data['creators'] = []
        if 'dcterms:creator' in j:
            creators = listify(j['dcterms:creator'])
        else:
            print('    warning: %s missing %s' % (base, 'creator'))
        for c1 in creators:
            c2 = {}
            if 'pgterms:agent' not in c1:
                print('    warning: %s missing %s' % (base, 'agent'))
                continue
            c1 = c1['pgterms:agent']
            assert 'pgterms:agent' not in c1
            c2['pg_url'] = 'http://www.gutenberg.org/ebooks/author/' + c1['@rdf:about'].split('/')[-1]
            c2['urls'] = []
            if 'pgterms:webpage' in c1:
                urls = listify(c1['pgterms:webpage'])
                for u in urls:
                    c2['urls'].append(u['@rdf:resource'])
            c2['birth'] = None
            if 'pgterms:birthdate' in c1:
                birth = rdf_val(c1['pgterms:birthdate'])
                assert len(birth) == 1
                c2['birth'] = int(birth[0])
            c2['death'] = None
            if 'pgterms:deathdate' in c1:
                death = rdf_val(c1['pgterms:deathdate'])
                assert len(death) == 1
                c2['death'] = int(death[0])
            c2['name'] = c1['pgterms:name']
            c2['aliases'] = []
            if 'pgterms:alias' in c1:
                c2['aliases'] = listify(c1['pgterms:alias'])
            data['creators'].append(c2)

        if 'dcterms:hasFormat' not in j:
            print('    error: %s missing %s' % (base, 'files'))
            print('    error: %s will be de-listed' % base)
            #error(j)
            data['files'] = []
            #yield base, data
            continue

        # second worst part
        files = []
        if base_type(j['dcterms:hasFormat']) == dict:
            print('    warning: %s has %s' % (base, 'weird files'))
            j['dcterms:hasFormat'] = [j['dcterms:hasFormat']]
        for f1 in j['dcterms:hasFormat']:
            f2 = {}
            assert len(f1) == 1
            f1 = f1['pgterms:file']
            try:
                # usually a psuedo-url that redirects
                f2['url'] = f1['@rdf:about']
                f2['format'] = rdf_val(f1['dcterms:format'])
                f2['modified'] = rdf_val(f1['dcterms:modified'])
                assert len(f2['modified']) == 1
                f2['modified'] = f2['modified'][0]
                f2['size'] = rdf_val(f1['dcterms:extent'])
                assert len(f2['size']) == 1
                f2['size'] = int(f2['size'][0])
            except:
                error(f1)
            files.append(f2)
        data['files'] = files
        assert len(files) > 0

        yield base, data


def downloads(rdf_files):
    "takes tarfile, returns (id, downloads) tuples"
    # yes, I am killing kittens by regexing xml
    # bulk XML is hard to do fast and this is "safe" input
    dl_num = re.compile('>([0-9]*)</pgterms:downloads>')
    for m, xml in rdf_iterator(rdf_files):
        d = dl_num.search(xml)
        if not d:
            print('error on', m.name)
            continue
        yield name, int(d.group(1))

def popular_books(limit=2000):
    popular = []
    tally = 0

    rdf = tarfile.open("rdf-files.tar.bz2")
    for n,dl in downloads(rdf):
        tally += 1
        if len(popular) == limit and (dl, n) < popular[-1]:
            continue
        # no need to be clever here
        # 90% of the runtime is iterating over the tarball
        # (though a binary-search insert or balanced tree is fun)
        popular.append((dl, n))
        popular.sort(reverse=True)
        popular = popular[:limit]
    rdf.close()
    print('processed %i items' % tally)
    return popular


def json_metadata():
    try:
        output = sys.argv[1]
        assert output not in  ('-h', '--help')
    except:
        print('Use: gutenberg.py output_file.json.gz')
        print('  Produces a lightweight summary of Project Gutenberg')
        sys.exit(1)
    rdf = tarfile.open("rdf-files.tar.bz2")
    everything = []
    for k,v in metadata(rdf):
        everything.append(v)
    with gzip.open(output, 'w') as g:
        json.dump(everything, g, indent=2, sort_keys=True)

def list_popular():
    # this is mostly pointless now, use something like
    # zcat pg.json.gz | jshon -a -e downloads -u -p -e id -u | paste -s -d '\t\n' | sort -n | tail -n 2000 | less
    # (it is faster too)
    try:
        limit = int(sys.argv[1])
        output = sys.argv[2]
    except:
        print('Use: gutenberg.py book_count output_file')
        print('  Outputs a list of the most downloaded IDs')
        print('  50000-ish total items, top 2000 seem notable')
        sys.exit(1)
    popular = popular_books(limit)
    with open(output, 'w') as outf:
        outf.write('\n'.join(list(zip(*popular))[1]))

if __name__ == '__main__':
    if not isfile('rdf-files.tar.bz2'):
        print("wget https://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.bz2")
        sys.exit(1)
    #list_popular()
    json_metadata()


