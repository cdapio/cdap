/**
 * Copyright © 2013 Cask Data, Inc.
 */

var util = require('util'),
    argv = require('optimist').argv,
    nock = require('nock');

var WebAppServer = require('../common/server');

/**
 * Set environment.
 */
process.env.NODE_ENV = 'development';

/**
 * Log level.
 */
var logLevel = 'INFO';
var devServer;

var DevServer = function() {
  this.cookieName = 'continuuity-local-edition';
  this.secret = 'local-edition-secret';
  this.productId = 'local';
  this.productName = 'Development Kit';
  DevServer.super_.call(this, __dirname, logLevel, 'Development UI', "developer");
};

util.inherits(DevServer, WebAppServer);

DevServer.prototype.startServer = function () {
  this.bindRoutes();

  this.server.listen(this.config['dashboard.bind.port']);

  this.logger.info('Listening on port', this.config['dashboard.bind.port']);

  /**
   * If mocks are enabled, use mock injector to simulate some responses.
   */
  var enableMocks = !!(argv.enableMocks === 'true');
  if (enableMocks) {
    this.logger.info('Webapp running with mocks enabled.');
    HttpMockInjector = require('../../test/httpMockInjector');
    if (!this.config['ssl.enabled'] === "true") {
      new HttpMockInjector(nock, this.config['router.server.address'], this.config['router.server.port']);
    } else {
      new HttpMockInjector(nock, this.config['router.server.address'], this.config['router.ssl.server.port']);
    }
  }
}

devServer = new DevServer();
/**
 * Export app.
 */
module.exports = devServer;