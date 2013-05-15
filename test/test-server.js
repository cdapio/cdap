/**
 * Copyright (c) 2013 Continuuity, Inc.
 *
 * Tests for developer side main.js.
 */
var assert = require('chai').assert,
    request = require('supertest');

//Changing environment to test.
process.env.NODE_ENV = "test";

var app = require('../server/developer/main.js');

describe('URL tests', function() {
  
  describe('GET /version', function() {
    it('should respond with json', function(done) {
      request(app)
        .get('/version')  
        .expect(200)
        .expect('Content-Type', /json/)
        .end(function(err, res) {
          assert.equal(err, null);
          assert.property(res.body, 'current');
          assert.property(res.body, 'newest');
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

  describe('POST /upload/:file', function() {
    it('should respond to file upload request', function(done) {
      request(app)
        .post('/upload/file')
        .expect(200, done);
    });
  });

});

