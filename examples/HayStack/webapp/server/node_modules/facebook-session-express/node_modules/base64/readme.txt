Update: this module may no longer be necessary as nodejs includes its own
base64 encoding/decoding functions.

This is a node.js C++ module that encodes and decodes to/from base64. Oh, and
compared to all other modules, it actually works.

It was written by Peteris Krumins (peter@catonmat.net).
His blog is at http://www.catonmat.net  --  good coders code, great reuse.

------------------------------------------------------------------------------

To build the module run:

    node-waf configure build

This will produce `base64.node` binary module. To use it, make sure the
module's directory is in NODE_PATH.

The module exports two functions `encode` and `decode`.

encode
------

Encodes a buffer to base64, returns encoded ascii string. Unlike all other
base64, it actually works well with binary data. If you're wondering why it
takes a buffer as argument, it's because there is no way to pass binary
strings to C++ code in a sane way.

Here is a basic example:

    var sys = require('sys');
    var base64_encode = require('base64').encode;
    var Buffer = require('buffer').Buffer;
    
    var buf = new Buffer('hello world');

    sys.print(base64_encode(buf));

    /* Output: aGVsbG8gd29ybGQ= */


decode
------

Decodes a buffer containing base64 string, or just a base64 string to original
data.

    var sys = require('sys');
    var base64_decode = require('base64').decode;

    sys.print(base64_decode('aGVsbG8gd29ybGQ='));

    /* Output: hello world */


------------------------------------------------------------------------------


SGF2ZSBmdW4hCg==


Sincerely,
Peteris Krumins
http://www.catonmat.net

