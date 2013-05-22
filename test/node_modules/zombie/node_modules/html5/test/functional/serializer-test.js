var test = require('tap').test
var HTML5 = require('../../lib/html5')

test('Serializer', function(t) {
    t.plan(1)
    var p = new HTML5.Parser()
    p.parse('<p class="Hello">Hi!</p><p class="hello there">Hi again</p>')
    t.equal(HTML5.serialize(p.tree.document, null, {lowercase: true}), '<html><head></head><body><p class="Hello">Hi!</p><p class="hello there">Hi again</p></body></html>');
    t.end()
})
