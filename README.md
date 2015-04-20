Some experiments that try to optimize the content served by Librarian.

The average content page has 10 images, and each image is uniquely used once per page.  So viewing a page will generate 11 requests.  Each requests requires a round trip (congesting the wifi network) and is served dynamically (requiring CPU).  By inlining images, a page needs a single request.  In theory this means Librarian could support 10x as many simultaneous users.

`uri_converter.py` embeds images as [data URIs](http://en.wikipedia.org/wiki/Data_URI_scheme).  On average this increases the size of a content zipball by 1.1%.  Browser support covers pretty much everything except IE 6/7/8, and those should gracefully degrade into either an image-less page (6 and 7) or large-image-less page (IE8).



