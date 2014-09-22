/**
 * Copyright © 2013 Cask Data, Inc.
 *
 * Browser testing for main page and streams page.
 * Uses Selenium + Firefox. Must start a remote instance of selenium available for download:
 * https://code.google.com/p/selenium/downloads/detail?name=selenium-server-standalone-2.32.0.jar
 * Download firefox:
 * http://www.mozilla.org/en-US/firefox/new/
 * Example command line:
 * java -jar <path to selenium.jar>
 * make test
 */
var assert = require('chai').assert,
    request = require('supertest'),
    webdriverjs = require('webdriverjs'),
    client = webdriverjs.remote();

//Changing environment to test.
process.env.NODE_ENV = 'test';

var devServer = require('../../server/developer/main.js');
var app = devServer.app;
var URI = 'http://127.0.0.1:9999/developer';

describe('Main app test', function() {

  before(function(done) {
    client.init(done);
  });

  beforeEach(function(done) {
    client.url(URI, done);
  });

  it('should show overview', function(done) {
    client
      .getTitle(function(err, title) {
        assert.isNull(err);
        assert.equal(title, 'Developer » Continuuity');
      })
      .isVisible('#modal-from-dom', function(err, visible) {
        assert.isFalse(visible);
      })
      .click('.overview')
      .waitFor('.panel-title', 1000)
      .getText('.panel-title', function(err, text) {
        assert.isNull(err);
        assert.isNotNull(text);
      })
      .element('css selector', 'div.sparkline-box-container', function(err, result) {
        assert.isNotNull(result.value.ELEMENT);
      })
      .click('.create-btn', function(err, btn) {
        assert.isNull(err);
      })
      .isVisible('#cmp-applications-list', function(err, visible) {
        assert.isNull(err);
        assert.isTrue(visible);
        done();
      });
  });

  it('should show streams', function(done) {
    client
      .click('.nav-streams')
      .waitFor('#cmp-streams-list', 1000)
      .isVisible('#cmp-streams-list', function(err, visible) {
        assert.isNull(err);
        assert.isTrue(visible);
        done();
      });
  });

  it('should show flows', function(done) {
    client
      .click('.nav-flows')
      .waitFor('#cmp-flows-list', 1000)
      .isVisible('#cmp-flows-list', function(err, visible) {
        assert.isNull(err);
        assert.isTrue(visible);
        done();
      });
  });

  it('should show datasets', function(done) {
    client
      .click('.nav-datasets')
      .waitFor('#cmp-datasets-list', 1000)
      .isVisible('#cmp-datasets-list', function(err, visible) {
        assert.isNull(err);
        assert.isTrue(visible);
        done();
      });
  });

  it('should show query page', function(done) {
    client
      .click('.nav-queries')
      .waitFor('#cmp-queries-list', 1000)
      .isVisible('#cmp-queries-list', function(err, visible) {
        assert.isNull(err);
        assert.isTrue(visible);
        done();
      });
  });

  after(function(done) {
    client.end(done);
  });

});