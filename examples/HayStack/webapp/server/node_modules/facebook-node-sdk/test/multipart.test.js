
var path = require('path');
var util = require('util');
var fs = require('fs');
var stream = require('stream');

var basedir = path.join(__dirname, '..');
var covdir = path.join(basedir, 'lib-cov');
var libdir = path.join(basedir, 'lib');

try {
  var stat = fs.statSync(covdir);
  if (stat.isDirectory()) {
    libdir = covdir;
  }
}
catch (e) {
}

var Multipart = require(path.join(libdir, 'multipart.js'));

module.exports = {

  constructor: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var multipart = new Multipart();
    assert.equal(multipart.dash.length, 2);
    assert.equal(multipart.dash.toString('ascii'), '--');
    assert.ok(multipart.boundary.length > 0);
    assert.ok(multipart.boundary.toString('ascii').match(/^[0-9a-z]+$/));

    done = true;
  },

  contentLength: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var multipart = new Multipart();
    multipart.addText('foo', 'bar');
    multipart.addFile('src', __filename, function(err) {
      assert.equal(err, null);

      function CounterStream() {
        this.writable = true;
        this.length = 0;
      }
      util.inherits(CounterStream, stream.Stream);
      CounterStream.prototype.write = function(data) {
        this.length += data.length
        return true;
      };
      CounterStream.prototype.end = function(data) {
        if (data) this.write(data);
        this.writable = false;
        return true;
      }
      CounterStream.prototype.destroy = function() {};
      CounterStream.prototype.destroySoon = function() {};

      var contentLength = multipart.getContentLength();

      var counter = new CounterStream();
      multipart.writeToStream(counter, function() {
        assert.equal(counter.length, contentLength);

        done = true;
      });
    });
  }

};

