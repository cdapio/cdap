/**
 * Copyright (c) 2013 Continuuity, Inc.
 *
 * Tests for enterprise side main.js.
 */
var assert = require('chai').assert,
    request = require('supertest');

//Changing environment to test.
process.env.NODE_ENV = "test";

var entServer = require('../server/enterprise/main.js');
var app = entServer.app;

describe('Node js tests', function() {

  describe('GET /version', function() {
    it('should respond with json', function(done) {
      request(app)
        .get('/version')
        .expect(200)
        .expect('Content-Type', /json/)
        .end(function(err, res) {
          assert.equal(err, null);
          var responseData = JSON.parse(res.text);
          assert.property(responseData, 'current');
          assert.property(responseData, 'newest');
          done();
        });
    });
  });

  describe('GET /destinations', function() {
    it('should get destinations', function(done) {
      request(app)
        .get('/destinations')
        .expect(200, done);
    });
  });

  describe('POST /credential', function() {
    it('should post credential', function(done){
      request(app)
        .post('/credential')
        .expect(200)
        .end(function(err, res) {
          assert.equal(err, null);
          assert.equal(res.text, 'true');
          done();
        });
    });
  });

  describe('Test entServer', function() {

    it('should test initial state', function(done) {
      assert.isTrue(entServer.configSet);
      assert.notEqual(entServer.config, {});
      assert.notEqual(entServer.app, {});
      assert.notEqual(entServer.server, {});
      done();
    });

    it('should get localhost address', function(done) {
      assert.equal(entServer.getLocalHost(), '127.0.0.1');
      done();
    });

    it('should get logger', function(done) {
      var logger = entServer.getLogger('console', 'Enterprise UI');
      assert.equal(logger.level.levelStr, 'INFO');
      assert.equal(logger.category, 'Enterprise UI');
      done();
    });

    it('should get io', function(done) {
      var io = entServer.getSocketIo(entServer.server);
      assert.isObject(io);
      done();
    });

  });


});
