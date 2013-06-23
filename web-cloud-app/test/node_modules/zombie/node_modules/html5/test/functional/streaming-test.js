var HTML5 = require('../../lib/html5'),
	events = require('events'),
	test = require('tap').test,
	serialize = require('../lib/serializeTestOutput').serializeTestOutput;

test('Streamed data', function(t) {
    t.plan(1)
    var em = new events.EventEmitter();
    var p = new HTML5.Parser();
    p.on('end', function() {
        t.equal(serialize(p.document), '<html>\n  <head>\n  <body>\n    <p>\n      "This is a test of the "\n      <em>\n        "emergency"\n      " broadcast system"\n')
        t.end()
    })
    p.parse(em);
    em.emit('data', '<p>This is a');
    em.emit('data', ' test of the <e');
    em.emit('data', 'm>emergency</em> broadcast system');
    em.emit('end');
})
