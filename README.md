Some experiments that try to optimize the content served by Librarian.

The average content page has 10 images, and each image is uniquely used once per page.  So viewing a page will generate 11 requests.  Each requests requires a round trip (congesting the wifi network) and is served dynamically (requiring CPU).  By inlining images, a page needs a single request.  In theory this means Librarian could support 10x as many simultaneous users.

`uri_converter.py` embeds images as [data URIs](http://en.wikipedia.org/wiki/Data_URI_scheme).  On average this increases the size of a content zipball by 1.1%.  Browser support covers pretty much everything except IE 6/7/8, and those should gracefully degrade into either an image-less page (6 and 7) or large-image-less page (IE8).

`gutenberg.py` converts the fifty thousand XML files that make up Project Gutenberg's metadata into a single json file, while performing some normalization.  It is 3x smaller (16MB) and 30x faster to process.  A decent computer will require 15 minutes for the conversion, and the resulting json file will take six seconds to load.  Ask Kyle for a copy of the file if you don't want to generate it yourself.

(If you notice that the json data is 3% smaller when produced by python3, don't worry.  `json.dump` in python2 likes to put a space after commas and this is the entire difference.  There is no data lost.)

