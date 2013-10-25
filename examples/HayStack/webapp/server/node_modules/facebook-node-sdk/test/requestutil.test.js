var path = require('path');
var http = require('http');
var https = require('https');
var testUtil = require('./lib/testutil.js');
var requestUtil = require(path.join(testUtil.libdir, 'requestutil.js'));

module.exports = {
  simpleRequest: function(beforeExit, assert) {
    var e = null;
    var d = null;
    var done = false;
    beforeExit(function() {
      assert.equal(e, null);
      assert.notEqual(d, null);
      assert.equal(JSON.parse(d).id, '1055572299');
      assert.ok(done);
    });

    var req = new requestUtil.requestFacebookApi(https, 'graph.facebook.com', 443, '/amachang', { method: 'GET' }, false, function(err, data) {
      e = err;
      d = data;
      done = true;
    });
  },

  simpleMultipartRequest: function(beforeExit, assert) {
    var e = null;
    var d = null;
    var done = false;
    beforeExit(function() {
      assert.equal(e, null);
      assert.notEqual(d, null);
      assert.equal(JSON.parse(d).id, '1055572299');
      assert.ok(done);
    });

    var req = new requestUtil.requestFacebookApi(https, 'graph.facebook.com', 443, '/amachang', { method: 'GET' }, true, function(err, data) {
      e = err;
      d = data;
      done = true;
    });
  },

  constructorAndStart: function(beforeExit, assert) {
    var e = null;
    var d = null;
    var done = false;
    beforeExit(function() {
      assert.equal(e, null);
      assert.notEqual(d, null);
      assert.equal(JSON.parse(d).id, '1055572299');
      assert.ok(done);
    });

    var req = new requestUtil.FacebookApiRequest(https, 'graph.facebook.com', 443, '/amachang', { method: 'GET' });
    req.start(false, function(err, data) {
      e = err;
      d = data;
      done = true;
    });
  },

  responseError: function(beforeExit, assert) {
    var e = null;
    var d = null;
    var done = false;
    beforeExit(function() {
      assert.notEqual(e, null);
      assert.equal(d, null);
      assert.equal(e.code, 'ENOTFOUND');
      assert.ok(done);
    });

    var req = new requestUtil.FacebookApiRequest(http, 'notfound.example.com', 80, '/', { method: 'GET' });

    req.start(false, function(err, data) {
      e = err;
      d = data;
      done = true;
    });
  },

  throwErrorAfterResponse: function(beforeExit, assert) {
    var e = null;
    var d = null;
    var done = false;
    beforeExit(function() {
      assert.notEqual(e, null);
      assert.equal(d, null);
      assert.equal(e.message, 'addListener only takes instances of Function');
      assert.ok(done);
    });

    var req = new requestUtil.FacebookApiRequest(https, 'graph.facebook.com', 443, '/amachang', { method: 'GET' });

    // break process
    req.selfBoundDataHandler = null;
    req.start(false, function(err, data) {
      e = err;
      d = data;
      done = true;
    });
  },

  dataError: function(beforeExit, assert) {
    var e = null;
    var d = null;
    var done = false;
    beforeExit(function() {
      assert.notEqual(e, null);
      assert.equal(d, null);
      assert.equal(e.message, 'dummy');
      assert.ok(done);
    });

    var req = new requestUtil.FacebookApiRequest(https, 'graph.facebook.com', 443, '/amachang', { method: 'GET' });

    req.afterResponse_ = req.afterResponse;
    req.afterResponse = function() {
      process.nextTick(function() {
        req.handleDataError(new Error('dummy'));
      });
      return req.afterResponse_.apply(this, arguments);
    };

    req.start(false, function(err, data) {
      e = err;
      d = data;
      done = true;
    });
  },

  throwErrorInEndData: function(beforeExit, assert) {
    var e = null;
    var d = null;
    var done = false;
    beforeExit(function() {
      assert.notEqual(e, null);
      assert.equal(d, null);
      assert.ok(done);
    });

    var req = new requestUtil.FacebookApiRequest(https, 'graph.facebook.com', 443, '/amachang', { method: 'GET' });

    req.detachDataAndEndAndErrorHandlers = null;
    req.start(false, function(err, data) {
      e = err;
      d = data;
      done = true;
    });
  }
};
