var path = require('path');
var express = require('express');
var testUtil = require('./lib/testutil.js');

var Facebook = require(path.join(testUtil.libdir, 'facebook.js'));

var config = testUtil.fbDefaultConfig;

module.exports = {

  clearAllPersistentData: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var app = express.createServer();
    app.configure(function () {
      app.use(express.bodyParser());
      app.use(express.cookieParser());
      app.use(express.session({ secret: 'foo bar' }));
      app.use(Facebook.middleware(config));
    });

    // Test clearAllPersistentData don't break session
    app.get('/', function(req, res) {
      req.facebook.clearAllPersistentData();
      if (req.session.cookie) {
        res.send('ok');
      }
      else {
        res.send('ng');
      }
    });

    assert.response(app, { url: '/' }, function(res) {
      assert.equal(res.body, 'ok');
      done = true;
    });
  },

  getPersistentData: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var app = express.createServer();
    app.configure(function () {
      app.use(express.bodyParser());
      app.use(express.cookieParser());
      app.use(Facebook.middleware(config));
    });

    // When there is no session, getPersistentData return defaultValue
    app.get('/', function(req, res) {
      var user = req.facebook.getPersistentData('user_id', 0);
      res.send(JSON.stringify(user));
    });

    assert.response(app, { url: '/' }, function(res) {
      assert.equal(res.body, '0');
      done = true;
    });
  },

  middleware: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var app = express.createServer();
    app.configure(function () {
      app.use(express.bodyParser());
      app.use(express.cookieParser());
      app.use(express.session({ secret: 'foo bar' }));
      app.use(Facebook.middleware(config));
    });

    app.get('/', function(req, res) {
      if (req.facebook) {
        res.send('ok');
      }
      else {
        res.send('ng');
      }
    });

    assert.response(app, { url: '/' }, function(res) {
      assert.equal(res.body, 'ok');
      done = true;
    });
  },

  loginRequired: function(beforeExit, assert) {
    var done = false;
    beforeExit(function() { assert.ok(done) });

    var app = express.createServer();
    app.configure(function () {
      app.use(express.bodyParser());
      app.use(express.cookieParser());
      app.use(express.session({ secret: 'foo bar' }));
      app.use(Facebook.middleware(config));
    });

    app.get('/', Facebook.loginRequired(), function(req, res) {
      req.facebook.getUser(function(err, user) {
        res.send(user);
      });
    });

    assert.response(app, { url: '/' }, function(res) {
      assert.equal(res.statusCode, 302);

      done = true;
    });
  }
};


