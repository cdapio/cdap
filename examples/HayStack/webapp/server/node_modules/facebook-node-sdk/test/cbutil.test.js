var path = require('path');
var testUtil = require('./lib/testutil.js');
var cb = require(path.join(testUtil.libdir, 'cbutil.js'));

module.exports = {

  callWrappedFunction: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    cb.wrap(setTimeout, true, 0)(function(err, d) {
      assert.equal(err, null);
      done = true;
    }, 10);
  },

  callDoubleWrappedFunction: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    cb.wrap(cb.wrap(setTimeout, true, 0), true, 0)(function(err, d) {
      assert.equal(err, null);
      done = true;
    }, 10);
  },

  throwsThenCallbackIsNotAFunction: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    assert.throws(function() {
      cb.wrap(setTimeout, true, 0)(null, 10);
    }, Error);

    done = true;
  },

  throwsInWrappeeFunction: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var wrapped = cb.wrap(function wrapped(callback) {
      throw Error('test');
    });

    wrapped(function callback(err, data) {
      assert.equal(data, null);
      assert.notEqual(err, null);
      assert.equal(err.message, 'test');
      done = true;
    });
  },

  callbackIsCalledOnce: function(beforeExit, assert) {
    var calledCount = 0;
    var errorCount = 0;

    var write_ = process.stderr.write
    beforeExit(function() {
      process.stderr.write = write_;
      assert.equal(calledCount, 1);
      assert.equal(errorCount, 2);
      assert.ok(done)
    });

    var log = cb.errorLog;
    cb.errorLog = function(msg) {
      errorCount++;
    };

    var wrapped = cb.wrap(function wrapped(callback) {
      callback(null, 1);
      callback(null, 2); // error 1;
    });

    wrapped(function callback(err, data) {
      calledCount++;
      done = true;
      throw Error('test'); // error 2
    });

    cb.errorLog = log;
  }

};


