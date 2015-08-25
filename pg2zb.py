#! /usr/bin/env python

import os, re, sys, gzip, json, time, zipfile, hashlib, subprocess
import urllib.request
from os.path import isfile
from itertools import *
from bs4 import BeautifulSoup
import uri_converter as uri

top_count = 1000
pg_size_limit = 1e6
debug = False
perform_downloads = True
perform_conversions = True  # of html to zipball
update_conversions = False
text_compression = 0.20  # arbitrary value for uncompressed size_limit scaling
pg_delay = 60
pg_skip = False
mirror = 'http://www.gutenberg.lib.md.us'
pg = []

LOCALES = [
    'gv', 'gu', 'gd', 'ga', 'gl', 'lg', 'ln', 'lo', 'tr', 'ts', 'tn', 'to',
    'lt', 'lu', 'th', 'ti', 'tg', 'te', 'ta', 'yo', 'de', 'ko', 'da', 'dz',
    'kn', 'el', 'eo', 'en', 'zh', 'ee', 'eu', 'zu', 'es', 'ru', 'rw', 'kl',
    'rm', 'rn', 'ro', 'be', 'bg', 'uk', 'ps', 'bm', 'bn', 'bo', 'br', 'bs',
    'ja', 'om', 'os', 'or', 'xh', 'ca', 'cy', 'cs', 'lv', 'pt', 'pa', 'is',
    'pl', 'hy', 'hr', 'hu', 'hi', 'ha', 'he', 'mg', 'uz', 'ml', 'mn', 'mk',
    'ur', 'mt', 'ms', 'mr', 'my', 'sq', 'aa', 've', 'af', 'vi', 'ak', 'am',
    'it', 'vo', 'ii', 'as', 'ar', 'et', 'ia', 'az', 'id', 'ig', 'ks', 'nl',
    'nn', 'nb', 'nd', 'ne', 'kw', 'nr', 'fr', 'fa', 'kk', 'ff', 'fi', 'fo',
    'ka', 'ss', 'sr', 'ki', 'sw', 'sv', 'km', 'st', 'sk', 'si', 'so', 'sn',
    'sl', 'ky', 'sg', 'se' ]

"""
bugs: size limit may be determined from non-compressed size
some html files are only available on g.org, and you will get a 24hr ban eventually
suggested method of logging 'pg2zb.py | tee log.txt'


98% of Text have html somewhere
10% are simple html page
80% are zipfile with single html and maybe images
10% are zipfile with multiple html and maybe images

ballpark of 50-100 books/day?
You have used Project Gutenberg quite a lot today or clicked through it really fast

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

runtime around 30 minutes per 1000
comment out 'broadcast' if loading directly
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

def call_status(cmd):
    "returns exit status"
    spp = subprocess.PIPE
    return subprocess.Popen(cmd, shell=False, stdout=spp, stderr=spp).wait()

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
    print('    DOWNLOADING', url)
    if 'gutenberg.org' in url:
        if pg_skip:
            print('    warning: postponing for another day')
            return
        print('    . . . .')
        time.sleep(pg_delay)
    fh = open(page_cache, 'wb')
    fh.write(pull(url))
    fh.close()

def pretty(thing):
    print(json.dumps(thing, indent=2, sort_keys=True))

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

preferred_types = ['application/zip', 'text/html', 'text/plain', 'charset=utf-8']

def best_file2(node):
    "returns the biggest most html-est thing it can find"
    files = node['files']
    for pref in preferred_types:
        attempt = []
        for f in files:
            form = f['format']
            if any(pref in a for a in form):
                attempt.append(f)
        if len(attempt) == 1:
            # a winner
            files = attempt
            break
        if len(attempt) == 0:
            # discard attempt
            continue
        # whittle down the attempt with another pass
        files = attempt
    if not files:
        return None
    # largest file is best file
    best = list(sorted((f['size'], f['url'], f) for f in files))[-1][-1]
    # but it still needs to be something we can use
    if not any('text/html' in a or 'text/plain' in a for a in best['format']):
        return None
    return best

def best_file(node):
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
                'images': 0,
                'timestamp': None,
                'broadcast': '$BROADCAST',
                'is_sponsored': False,
                'is_partner': True,
                'archive': 'core',
                'keep_formatting': False,
                'publisher': 'Project Gutenburg',
                'license':  'PD',
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

def extract_text(page_cache, text_path):
    assert zipfile.is_zipfile(page_cache)
    z = zipfile.ZipFile(page_cache, 'r')
    text = [f for f in z.namelist() if f.lower().endswith('.txt')]
    assert len(text) == 1
    temp = open(text_path, 'wb')
    temp.write(z.read(text[0]))
    temp.close()

def text_to_html(node, text_path, html_path):
    "kind of slow, cache the html"
    # would be nice if there was ToC support for HTML output
    if good_file(html_path) and not update_conversions:
        return
    authors = ' & '.join(c['name'] for c in node['creators'])
    if not authors:
        authors = 'unknown'
    cmd = ['GutenMark', '--debug', '--yes-header',
           '--title="%s"' % node['title'], '--author="%s"' % authors,
           text_path, html_path]
    print('    running gutenmark on %s' % node['id'])
    call_status(cmd)

def get_language(node):
    # 0.3% of books have multiple languages
    # this function always assumes a single language
    lang = node['language'][0].lower().strip()
    if lang in LOCALES:
        return lang
    print('    warning: %s contains an improper language key' % node['id'])
    return ''

def get_keywords(node):
    import re
    subjects = []
    for subject in node['subjects']:
        subjects.extend(subject.split(' -- '))
    subjects.extend(node['bookshelf'])
    subjects = [re.sub(r'[^\w\d]', ' ', x) for x in subjects]
    subjects = [re.sub(r'\s+', ' ', x) for x in subjects]
    subjects = [re.sub(r'\s\w\s', ' ', x) for x in subjects]
    return ', '.join(subjects)

def simple_zipball(node, html_path, encoding=None):
    "single html file, no images"
    uniq = node_md5(node)
    zip_path = os.path.join('zipballs', uniq + '.zip')
    if good_file(zip_path) and not update_conversions:
        return
    z = zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED)
    stamp = timestamp(html_path)
    info = build_info(node['base_url'], keywords=get_keywords(node),
           language=get_language(node), title=node['title'].strip(),
           timestamp=stamp, license=node['license'])
    z.writestr(os.path.join(uniq, 'info.json'), json.dumps(info))
    if encoding:
        utf8_html = open(html_path).decode(encoding).encode('utf8').read()
    else:
        utf8_html = open(html_path).read()
    z.writestr(os.path.join(uniq, 'index.html'), utf8_html)
    z.close()
    print('    ' + uniq + '.zip')

def find_htmls(z):
    return list(uri.find_html(z))

def lazy_rename(n, uniq):
    "convert old zip paths to new zip paths"
    return uniq + '/' + n.partition('/')[2]

def fancy_zipball(node, pgzip_path):
    "single or multiple html page in zip file, possibly with images"
    uniq = node_md5(node)
    zip_path = os.path.join('zipballs', uniq + '.zip')
    if good_file(zip_path) and not update_conversions:
        return
    z1 = zipfile.ZipFile(pgzip_path, 'r')
    pages = list(find_htmls(z1))
    if len(pages) == 1:
        page = pages[0]
    else:
        number = os.path.basename(node['id'])
        page = [p for p in pages if number in os.path.basename(p.filename)]
        assert len(page) == 1
        page = page[0]
    # 'page' is special and will be renamed to index.html
    old_index = os.path.basename(page.filename)
    to_skip = uri.files_to_skip(z1)
    replaced = set()
    z2 = zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED)
    for i in uri.find_html(z1):
        try:
            # data-uri the images
            html2, r2 = uri.process_html(z1, i, to_skip)
        except RuntimeError:
            # some of the html sends Soup into an infinite recursion
            print('    error: %s broke the soup' % node['id'])
            z2.close()
            z1.close()
            os.remove(zip_path)
            return
        replaced |= r2
        # add all the files
        new_name = lazy_rename(i.filename, uniq)
        if i == page:
            new_name = os.path.join(uniq, 'index.html')
        if i != page and old_index in html2:
            # never seems to happen?
            print('    error: %s has broken link to %s' % (node['id'], old_index))
        z2.writestr(new_name, html2)
        replaced.add(i.filename)
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
    info = build_info(node['base_url'], title=node['title'].strip(),
           language=get_language(node), keywords=get_keywords(node),
           timestamp=stamp)
    z2.writestr(os.path.join(uniq, 'info.json'), json.dumps(info))
    z2.close()
    z1.close()
    print('    ' + uniq + '.zip')

def multipage_zipball(node, pgzip_path):
    "multiple html pages in zip file, possibly with images"
    # only 6 out of the top 1000 use this
    print('    note: %s is multi-page document' % node['id'])
    fancy_zipball(node, pgzip_path)

def get_encoding(file_node):
    return re.search('^charset\= (*+)', file_node['format'][1])

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
    rank = list((n['downloads'], n['id'], n) for n in pg)
    rank.sort()
    rank.reverse()
    return list(n for _,_,n in rank[:number])

def legit_filter(nodes, quiet=False):
    "text-based, public domain and html/txt available"
    if not quiet:
        print('size at top:', len(nodes))
    nodes = list(n for n in nodes if n['media_type'] == 'Text')
    if not quiet:
        print('size at text:', len(nodes))

    pubd_fn = lambda s: s.startswith('Public domain')
    nodes = list(tag_filter(nodes, 'license', pubd_fn))
    if not quiet:
        print('size at public:', len(nodes))

    good_format = lambda f: f.startswith('text/html') or f.startswith('text/plain')
    html_fn = lambda formats: any(good_format(f) for f in formats)
    nodes = list(tag_filter(nodes, ['files', '*', 'format'], html_fn))
    if not quiet:
        print('size at format:', len(nodes))
    return nodes

def process_node(n):
    print(n['id'])
    promising = best_file2(n)
    if not promising:
        print('    warning: %s has no usable text' % n['id'])
        return
    scale = 1
    if not any('application/zip' in a for a in promising['format']):
        scale = text_compression
    if (promising['size'] * scale) > pg_size_limit:
        print('    warning: %s is too large' % n['id'])
        return
    url = promising['url']
    if 'gutenberg.org/files/' in url:
        url = node_to_mirror(n, promising)
    page_cache = url_to_local(url)
    cache_hit(url, page_cache)
    if perform_downloads and not good_file(page_cache):
        print('    warning: %s did not download' % n['id'])
        return
    if promising['size'] != os.path.getsize(page_cache):
        print('    warning: %s is wrong size' % n['id'])
    if not perform_conversions:
        return
    if any('text/plain' in a for a in promising['format']):
        # simple single text file
        if url.endswith('.txt'):
            text_path = page_cache
            html_path = page_cache.replace('.txt', '.html')
        elif url.endswith('.zip'):
            text_path = 'temp.txt'
            extract_text(page_cache, text_path)
            html_path = page_cache.replace('.zip', '.html')
        else:
            print('    error: %s is a weird text file' % n['id'])
            return
        assert html_path != page_cache
        text_to_html(n, text_path, html_path)
        try:
            simple_zipball(n, html_path)
        except UnicodeDecodeError:
            simple_zipball(n, html_path, encoding=get_encoding(promising))
        return
    assert any('text/html' in a for a in promising['format'])
    if not url.endswith('.zip'):
        # simple single html file
        try:
            simple_zipball(n, page_cache)
        except UnicodeDecodeError:
            simple_zipball(n, page_cache, encoding=get_encoding(promising))
        return
    if not zipfile.is_zipfile(page_cache):
        print('    error: %s not a zip file' % n['id'])
        return
    try:
        z1 = zipfile.ZipFile(page_cache, 'r')
    except zipfile.BadZipFile:
        print('    error: %s not a zip file' % n['id'])
        return
    page_count = len(find_htmls(z1))
    z1.close()
    assert page_count > 0
    if page_count == 1:
        fancy_zipball(n, page_cache)
        return
    multipage_zipball(n, page_cache)

def main():
    init()
    nodes = most_popular(top_count)
    #nodes = pg
    nodes = legit_filter(nodes)

    for n in nodes:
        if debug:
            process_node(n)
            continue
        try:
            process_node(n)
        except KeyboardInterrupt:
            break
        except:
            print('    ERROR: %s unknown error' % n['id']) 
            continue

if __name__ == '__main__':
    main()


