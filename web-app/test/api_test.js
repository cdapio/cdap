/**
 * Copyright © 2013 Cask Data, Inc.
 *
 * Tests for api.js.
 */
var assert = require('chai').assert;

//Changing environment to test.
process.env.NODE_ENV = "test";

describe('Test API functions', function() {

  before(function(done) {
    API = require('../server/common/api');
    done();
  });

  describe('configure', function() {
    it('should set config and credential', function(done) {
      var config = {"path": 1};
      var credential = "abcdef";
      API.configure(config, credential);
      assert.equal(API.config, config);
      assert.equal(API.credential, credential);
      done();
    });
  });

});