#! /usr/bin/env python

import os, sys, gzip, json, time, zipfile, hashlib
import urllib.request
from os.path import isfile
from itertools import *
from bs4 import BeautifulSoup
import uri_converter as uri

top_count = 1000
pg_size_limit = 1e6
perform_downloads = True
perform_conversions = True  # of html to zipball
pg_delay = 60
pg_skip = False
mirror = 'http://www.gutenberg.lib.md.us'
pg = []

"""
bugs: size limit may be determined from non-compressed size
some html files are only available on g.org, and you will get a 24hr ban eventually
suggested method of logging 'pg2zb.py > log.txt'


98% of Text have html somewhere
10% are simple html page
80% are zipfile with single html and maybe images
10% are zipfile with multiple html and maybe images

ballpark of 50-100 books/day?

top: 500
text: 499
public: 492
html: 485
time: 560 seconds
430 files
8 broken soups
8 multi-page
42 very large

ALL FILES
size at top: 49548
size at text: 48308
size at public: 47805
size at html: 47659
1063 very large
800 broken (estimated)
93% of the collection
"""

def init():
    global pg
    if not isfile('pg.json.gz'):
        print('acquire pg.json.gz')
        sys.exit(1)

    pg = json.load(gzip.open('pg.json.gz', 'rt'))

    for d in 'cache zipballs'.split():
        try:
            os.mkdir(d)
        except:
            pass

pull = lambda url: urllib.request.urlopen(url).read()

def good_file(path):
    if not os.path.exists(path):
        return False
    return os.stat(path).st_size > 0

def cache_hit(url, page_cache):
    "download logic"
    if good_file(page_cache):
        return
    if not perform_downloads:
        return
    print('DOWNLOADING', url)
    if 'gutenberg.org' in url:
        if pg_skip:
            print('    warning: %s is for another day' % n['id'])
            return
        print('    . . . .')
        time.sleep(pg_delay)
    fh = open(page_cache, 'wb')
    fh.write(pull(url))
    fh.close()

def pretty(thing):
    print(json.dumps(thing, indent=2, sort_keys=True))

def extract(id_set):
    "extract elements from pg.json, not very efficient for N=all"
    # could be more clever, destroys the ordering
    return [n for n in pg if n['id'] in id_set]

def tag_filter(nodes, tag, test_fn):
    "tag can be a single string or a list, returns items that pass test_fn"
    if type(tag) == str:
        tag = [tag]
    for n in nodes:
        n2 = [dict(n)]
        for t in tag:
            if t == '*':  # for lists
                n2 = list(chain.from_iterable(n2))
                #n2 = [b for a in n2 for b in a]
                continue
            n2 = [a[t] for a in n2]
        if any(test_fn(a) for a in n2):
            yield n

def best_file(node):
    "returns the biggest most html-est thing it can find"
    # todo: which charsets are the best?
    files = node['files']
    attempt1 = []  # zipped html (hopefully has images)
    attempt2 = []  # single html page
    for f in files:
        form = f['format']
        if not any('text/html' in a for a in form):
            continue
        attempt2.append(f)
        if not any('application/zip' in a for a in form):
            continue
        attempt1.append(f)
    if len(attempt1) == 0:
        attempt1 = attempt2
    if len(attempt1) > 1:
        # url is in there as a sorting tiebreaker
        return list(sorted((f['size'], f['url'], f) for f in attempt1))[-1][-1]
    if len(attempt1) == 1:
        return attempt1[0]
    return None

def number_to_url(number):
    "assumes everything is in the new +10000 format"
    number = str(number)
    x = int(number)
    if x < 10:
        return os.path.join(mirror, '0', number)
    return os.path.join(*([mirror] + list(number)[:-1] + [number]))

def build_info(url, **kwargs):
    "probably flawed, derived from sample zipballs"
    info = {}
    info['url'] = url
    info['domain'] = url.partition('//')[2].split('/')[0]
    defaults = {'title': None,
                'is_sponsored': False,
                'images': 0,
                'timestamp': None,
                'archive': 'core',
                'keep_formatting': False,
                'parter': None,
                'license': None,
               }
    for k,v in defaults.items():
        info[k] = v
    for k,v in kwargs.items():
        info[k] = v
    return info

def url_to_local(url):
    return os.path.join('cache', url.replace('/', '\\'))

def node_to_mirror(node, f):
    "provides mirror address"
    url = f['url']
    number = node['id'].split('/')[-1]
    base_name = url.split('/')[-1]
    return os.path.join(number_to_url(number), base_name)

def node_md5(node):
    return hashlib.md5(node['base_url'].encode('utf8')).hexdigest()

def timestamp(path):
    "of the file at given path"
    mtime = time.gmtime(os.path.getmtime(path))
    return time.strftime('%Y-%m-%d %H:%M:%S UTC', mtime)

def simple_zipball(node, html_path):
    "single html file, no images"
    uniq = node_md5(node)
    zip_path = os.path.join('zipballs', uniq + '.zip')
    z = zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED)
    stamp = timestamp(html_path)
    info = build_info(node['base_url'], title=node['title'].strip(), timestamp=stamp, license=node['license'])
    z.writestr(os.path.join(uniq, 'info.json'), json.dumps(info))
    z.writestr(os.path.join(uniq, 'index.html'), open(html_path).read())
    z.close()

def find_htmls(z):
    return list(uri.find_html(z))

def lazy_rename(n, uniq):
    "convert old zip paths to new zip paths"
    return uniq + '/' + n.partition('/')[2]

def fancy_zipball(node, pgzip_path):
    "single html page in zip file, possibly with images"
    z1 = zipfile.ZipFile(pgzip_path, 'r')
    pages = list(find_htmls(z1))
    assert len(pages) == 1
    page = pages[0]
    to_skip = uri.files_to_skip(z1)
    try:
        # data-uri the images
        html2, replaced = uri.process_html(z1, page, to_skip)
    except RuntimeError:
        # some of the html sends Soup into an infinite recursion
        print('    error: %s broke the soup' % node['id'])
        return
    uniq = node_md5(node)
    zip_path = os.path.join('zipballs', uniq + '.zip')
    z2 = zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED)
    # add all the files
    z2.writestr(os.path.join(uniq, 'index.html'), html2)
    replaced.add(page)
    img_tally = 0
    # probably should flatten directory structure
    for i in z1.infolist():
        n = i.filename
        if n in to_skip:
            uri.zip_rename(z1, z2, i, lazy_rename(n, uniq))
            if uri.is_data(n):
                img_tally += 1
            continue
        if n in replaced:
            continue
        if uri.is_data(n):
            img_tally += 1
        uri.zip_rename(z1, z2, i, lazy_rename(n, uniq))
    stamp = timestamp(pgzip_path)
    info = build_info(node['base_url'], title=node['title'].strip(), timestamp=stamp, license=node['license'], images=img_tally)
    z2.writestr(os.path.join(uniq, 'info.json'), json.dumps(info))
    z2.close()
    z1.close()

def multipage_zipball(node, pgzip_path):
    "multiple html pages in zip file, possibly with images"
    # at some point this will replace the older fancy_zipball() use case too
    # only 6 out of the top 1000 use this
    print('    warning: %s is multi-page document' % node['id'])
    pass

"""
challenge one: figure out which file types are worth getting
these ones look promising:

['application/zip', 'text/html']
['application/zip', 'text/html; charset=iso-8859-1']
['application/zip', 'text/html; charset=us-ascii']
['application/zip', 'text/html; charset=utf-8']
['application/zip', 'text/html; charset=windows-1251']
['application/zip', 'text/html; charset=windows-1252']
['application/zip', 'text/plain']
['application/zip', 'text/plain; charset=iso-8859-1']
['application/zip', 'text/plain; charset=us-ascii']
['application/zip', 'text/plain; charset=utf-8']
['application/zip', 'text/plain; charset=windows-1251']
['application/zip', 'text/plain; charset=windows-1252']
['application/zip', 'text/rtf']
['application/zip', 'text/xml']
['application/zip', 'text/x-rst']  # restructured text, with images
['text/html']
['text/html; charset=iso-8859-1']
['text/html; charset=us-ascii']
['text/html; charset=utf-8']
['text/html; charset=windows-1251']
['text/html; charset=windows-1252']
['text/plain']
['text/plain; charset=iso-8859-1']
['text/plain; charset=us-ascii']
['text/plain; charset=utf-8']
['text/plain; charset=windows-1251']
['text/plain; charset=windows-1252']
['text/rtf']
['text/xml']
['text/x-rst']

"""

def most_popular(number):
    "top N items by download count"
    if number >= len(pg):
        return pg
    rank = list((n['downloads'], n['id']) for n in pg)
    rank.sort()
    rank.reverse()
    top = list(b for a,b in rank[:number])
    return extract(top)

def legit_filter(nodes, quiet=False):
    "text-based, public domain and html availible"
    if not quiet:
        print('size at top:', len(nodes))
    nodes = list(n for n in nodes if n['media_type'] == 'Text')
    if not quiet:
        print('size at text:', len(nodes))

    pubd_fn = lambda s: s.startswith('Public domain')
    nodes = list(tag_filter(nodes, 'license', pubd_fn))
    if not quiet:
        print('size at public:', len(nodes))

    # easy mode: html only
    html_fn = lambda formats: any(f.startswith('text/html') for f in formats)
    nodes = list(tag_filter(nodes, ['files', '*', 'format'], html_fn))
    if not quiet:
        print('size at html:', len(nodes))
    return nodes

def main():
    init()
    nodes = most_popular(top_count)
    #nodes = pg
    nodes = legit_filter(nodes)

    for n in nodes:
        print(n['id'])
        promising = best_file(n)
        if not promising:
            print('    warning: %s has no html' % n['id'])
            continue
        if promising['size'] > pg_size_limit:
            print('    warning: %s is too large' % n['id'])
            continue
        url = promising['url']
        if url.endswith('-h.zip'):
            url = node_to_mirror(n, promising)
        page_cache = url_to_local(url)
        cache_hit(url, page_cache)
        if perform_downloads and not good_file(page_cache):
            print('    warning: %s did not download' % n['id'])
            continue
        if promising['size'] != os.path.getsize(page_cache):
            print('    warning: %s is wrong size' % n['id'])
        if not perform_conversions:
            continue
        if not url.endswith('.zip'):
            # simple single html file
            simple_zipball(n, page_cache)
            continue
        z1 = zipfile.ZipFile(page_cache, 'r')
        page_count = len(find_htmls(z1))
        z1.close()
        assert page_count != 0
        if page_count == 1:
            fancy_zipball(n, page_cache)
            continue
        multipage_zipball(n, page_cache)

if __name__ == '__main__':
    main()

