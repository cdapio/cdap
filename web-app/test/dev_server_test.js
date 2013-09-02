/**
 * Copyright (c) 2013 Continuuity, Inc.
 *
 * Tests for developer side main.js.
 */
var assert = require('chai').assert,
    request = require('supertest');

//Changing environment to test.
process.env.NODE_ENV = "test";

var devServer = require('../server/local/main.js');
var app = devServer.app;

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

  // describe('POST /upload/:file', function() {
  //   it('should respond to file upload request', function(done) {
  //     setTimeout(function() {
  //       request(app)
  //         .post('/upload/file')
  //         .expect(200, done);
  //     }, 3000);
  //   });
  // });

  describe('Test devServer', function() {

    it('should test initial state', function(done) {
      assert.isTrue(devServer.configSet);
      assert.notEqual(devServer.config, {});
      assert.notEqual(devServer.app, {});
      assert.notEqual(devServer.server, {});
      done();
    });

    it('should get localhost address', function(done) {
      assert.equal(devServer.getLocalHost(), '127.0.0.1');
      done();
    });

    it('should get logger', function(done) {
      var logger = devServer.getLogger();
      assert.equal(logger.level.levelStr, 'INFO');
      assert.equal(logger.category, 'Developer UI');
      done();
    });

    it('should get io', function(done) {
      var io = devServer.getSocketIo(devServer.server);
      assert.isObject(io);
      done();
    });

  });


});
