#! /usr/bin/env python

import os, re, sys, base64, zipfile
from os.path import isdir, isfile, dirname, basename, splitext

from bs4 import BeautifulSoup

help_string = """\
Use:
python converter.py path/to/a/page.zip
python converter.py path/to/many/zips/

Embeds page content inline as data URIs.
Can take any number and combination of files and directories.
Revised versions of the pages will be saved to the current directory.
The utility will never overwrite an existing zipball.

Tested against everything at archive.outernet.is
Processes 37.2 MB/minute of content on a wimpy laptop.
(python3 is 15% slower than python2)
Average increase in zipball size of 1.1%
Average page has 10 images, in theory will boost performance 10x.

BUGS:
The info.json file will have an incorrect image field.
Probably misses some edge cases.
Could be 40% faster, soups every page twice.
Only supports images in <img> tags.
"""

data_extensions = 'jpg png gif'
data_extensions = set('.'+e for e in data_extensions.split())

html_extensions = 'html htm'
html_extensions = set('.'+e for e in html_extensions.split())

zf = zipfile.ZipFile

def iszip(path):
    return path.endswith('.zip') and \
           isfile(path) and \
           zipfile.is_zipfile(path)

def zips_to_process(args):
    for a in args:
        if iszip(a):
            yield a
            continue
        if not isdir(a):
            continue
        for f in os.listdir(a):
            path = os.path.join(a,f)
            if iszip(path):
                yield path

def find_html(z):
    "takes a zipfile object, returns info objects"
    for i in z.infolist():
        n = basename(i.filename)
        _,e = splitext(n)
        if e not in html_extensions:
            continue
        yield i

def smart_join(path1, path2):
    "strips out /../"
    # probably not smart enough to handle escapes
    while path2.startswith('./'):
        path2 = path2[2:]
    p3 = os.path.join(path1, path2)
    while p3.startswith('./'):
        p3 = p3[2:]
    if p3.startswith('../'):
        raise Exception('Path %s goes above root' % path1)
    while '/../' in p3:
        p3 = re.sub('/[^/]*/\.\./', '/', p3, count=1)
    return p3

def files_to_skip(z, limit=1e6):
    "set of absolute zip names that exceed the limit size"
    size = dict((i.filename, i.file_size) for i in z.infolist())
    total = dict((i.filename, 0) for i in z.infolist())
    for i in find_html(z):
        name = i.filename
        html = z.open(i).read()
        soup = BeautifulSoup(html)
        base_path = dirname(name)
        # ignore html files
        size.pop(name)
        total.pop(name)
        for img in soup.find_all('img'):
            if img['src'].startswith('data:'):
                continue
            # figure out the absolute path
            img_path = smart_join(base_path, img['src'])
            total[img_path] += size[img_path]
    skip = set()
    skip.update(n for n,s in size.items() if s > limit)
    skip.update(n for n,s in total.items() if s > limit)
    return skip

def encode_file(z, abs_name):
    b64 = base64.b64encode(z.open(abs_name).read())
    return b64.decode()

def data_url(mime, b64):
    return 'data:%s;base64,%s' % (mime, b64)

def mime_table(tag, ext):
    "very incomplete"
    table = {('img', 'png'): 'image/png',
             ('img', 'jpg'): 'image/jpg',
             ('img', 'gif'): 'image/gif',}
    return table[(tag, ext)]

def image_data_uri(img_soup, b64):
    "modifies the soup in place"
    ext = splitext(img_soup['src'])[1].strip('.')
    mime = mime_table('img', ext)
    img_soup['src'] = data_url(mime, b64)

def process_html(z, i, to_skip):
    "returns (new_html, replaced_files)"
    replaced = set()
    root_path = dirname(i.filename)
    html = z.open(i).read()
    soup = BeautifulSoup(html)
    for img in soup.find_all('img'):
        if img['src'].startswith('data:'):
            continue
        n = smart_join(root_path, img['src'])
        if n in to_skip:
            continue
        replaced.add(n)
        b64 = encode_file(z, n)
        image_data_uri(img, b64)
    return str(soup), replaced

def zip_copy(z1, z2, i):
    z2.writestr(i, z1.open(i).read())

def main(zips):
    for zipname in zips:
        z2_name = basename(zipname)
        if isfile(z2_name):
            print("Skipping %s" % z2_name)
            continue
        print("Converting %s" % z2_name)
        z1 = zf(zipname, 'r')
        try:
            to_skip = files_to_skip(z1)
        except KeyError:
            print("    ERROR: MISSING FILES")
            continue
        z2 = zf(z2_name, 'w')
        replaced = set()
        # insert data URIs
        for i in find_html(z1):
            html2, r2 = process_html(z1, i, to_skip)
            replaced |= r2
            #open('test.html', 'w').write(html2)
            z2.writestr(i, html2)
            replaced.add(i.filename)
        # add in non-uri files
        for i in z1.infolist():
            n = i.filename
            if n in to_skip:
                zip_copy(z1, z2, i)
                continue
            if n in replaced:
                continue
            zip_copy(z1, z2, i)
        z2.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        main(zips_to_process(sys.argv[1:]))
    else:
        print(help_string)

