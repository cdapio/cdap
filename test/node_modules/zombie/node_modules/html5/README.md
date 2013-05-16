HTML5 Parser for node.js
========================


Example (With jQuery!) 
----------------------

        /* Before you run this, run:
              git submodule update --init
              (cd deps/jquery; rake)
        */
        var  HTML5 = require('html5'),
            Script = process.binding('evals').Script,
              util = require('util'),
                fs = require('fs'),
             jsdom = require('jsdom'),
            window = jsdom.jsdom(null, null, {parser: HTML5}).createWindow()

        var parser = new HTML5.Parser({document: window.document});

        var inputfile = fs.readFileSync('doc/jquery-example.html');
        parser.parse(inputfile);

        jsdom.jQueryify(window, __dirname + '/deps/jquery/dist/jquery.js', function(window, jquery) {
                Script.runInNewContext('jQuery("p").append("<b>Hi!</b>")', window);
                util.puts(window.document.innerHTML);

        });

Interesting features
--------------------

* Streaming parser: You can pass `parser.parse` an `EventEmitter` and the
  parser will keep adding data as it's received.

* HTML5 parsing algorithm. If you find something this can't parse, I'll want
  to know about it. It should make sense out of anything a browser can.

Installation
-------------

Use `npm`, or to use the git checkout, read on.

You'll need to fetch dependencies or initialize git submodules if you're
pulling this from my git repository. 

	npm install

and give it a run:

	npm test

(At time of this writing, 1800 tests pass)

Git repository at http://dinhe.net/~aredridel/projects/js/html5.git/
