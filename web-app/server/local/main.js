/**
 * Copyright (c) 2013 Cask Data, Inc.
 */

var util = require('util'),
  fs = require('fs'),
  http = require('http'),
  https = require('https'),
  promise = require('q'),
  sys = require('sys'),
  argv = require('optimist').argv,
  lodash = require('lodash'),
  nock = require('nock');

var WebAppServer = require('../common/server'),
    configParser = require('../common/configParser');

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
  DevServer.super_.call(this, __dirname, logLevel, 'Development UI');
  this.extractConfig()
      .then(function () {
        this.setUpServer();
      }.bind(this));
};

util.inherits(DevServer, WebAppServer);

DevServer.prototype.extractConfig = configParser.extractConfig;

DevServer.prototype.setUpServer = function setUpServer() {
  this.setAttrs();
  this.Api.configure(this.config, this.apiKey || null);
  this.launchServer();
}
DevServer.prototype.setAttrs = function() {
  if (this.config['dashboard.https.enabled'] === "true") {
    this.lib = https;
  } else {
    this.lib = http;
  }
  this.apiKey = this.config.apiKey;
  this.version = this.config.version;
  this.cookieName = 'continuuity-local-edition';
  this.secret = 'local-edition-secret';
  this.setCookieSession(this.cookieName, this.secret);
  this.configureExpress();
}


DevServer.prototype.launchServer = function() {
   var key,
       cert,
       options = this.configureSSL() || {};
   this.server = this.getServerInstance(options, this.app);
   //LaunchServer and then StartServer?? Kind of redundant on names. Any alternative is welcome.
   this.setEnvironment('local', 'Development Kit', this.version, this.startServer.bind(this));
}

DevServer.prototype.configureSSL = function () {
  var options = {};
  if (this.config['dashboard.https.enabled'] === "true") {
    key = this.config['dashboard.ssl.key'],
    cert = this.config['dashboard.ssl.cert'];
    options = {
      key: fs.readFileSync(key),
      cert: fs.readFileSync(cert),
      requestCert: false,
      rejectUnauthorized: false
    };
    this.config['dashboard.bind.port'] = this.config['dashboard.bind.port.ssl'];
  }
  return options;
}

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
    if (!this.config['dashboard.https.enabled'] === "true") {
      new HttpMockInjector(nock, this.config['router.server.address'], this.config['router.server.bind.port']);
    } else {
      new HttpMockInjector(nock, this.config['router.server.address'], this.config['router.server.bind.ssl.port']);
    }
  }
}

/**
 * Catch anything uncaught.
 */
process.on('uncaughtException', function (err) {
  devServer.logger.info('Uncaught Exception', err);
});

devServer = new DevServer();
/**
 * Export app.
 */
module.exports = devServer;