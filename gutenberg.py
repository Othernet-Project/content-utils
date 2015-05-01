#! /usr/bin/env python

import re, sys, tarfile

"""
https://www.gutenberg.org/cache/epub/feeds/rdf-files.tar.bz2

todo:

automatically download rdf
automatically re-download if stale

filter by media type (only books?  prefer html? prefer epub?)
    <dcterms:type> ... <rdf:value>
    <dcterms:hasFormat> ... <rdf:value>  (multiple of these)

generate filelist
rsync filelist
    though keep files that just fell off of most-popular

convert new/updated files to zipball
extract metadata for bookshelf app
"""

def downloads(rdf_files):
    "takes tarfile, returns (id, downloads) tuples"
    # yes, I am killing kittens by regexing xml
    # bulk XML is hard to do fast and this is "safe" input
    dl_num = re.compile('>([0-9]*)</pgterms:downloads>')
    id_num = re.compile('pg([^.]*).rdf')
    skips = set('0 999999'.split())
    for m in rdf_files:
        if not m.name.endswith('.rdf'):
            continue
        name = id_num.search(m.name).group(1)
        if name in skips:
            continue
        xml = rdf_files.extractfile(m).read()
        if sys.version_info >= (3, 0):
            xml = xml.decode()
        d = dl_num.search(xml)
        if not d:
            print('error on', m.name)
            continue
        yield name, int(d.group(1))

limit = 2000
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

with open('popular', 'w') as popf:
    popf.write('\n'.join(list(zip(*popular))[1]))

