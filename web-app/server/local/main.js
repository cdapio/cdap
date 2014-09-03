/**
 * Copyright (c) 2013 Continuuity, Inc.
 */

var util = require("util"),
  fs = require('fs'),
  http = require('http'),
  https = require('https'),
  promise = require('q'),
  sys = require('sys'),
  argv = require('optimist').argv,
  nock = require('nock');

var WebAppServer = require('../common/server');

// Default port for the Dashboard.
var DEFAULT_BIND_PORT = 9999;

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
  DevServer.super_.call(this, __dirname, logLevel);
  this.getConfig(__dirname + '/continuuity-local.xml')
      .then(this.setUpServer.bind(this));
};
util.inherits(DevServer, WebAppServer);

DevServer.prototype.setAttrs = function(configuration) {
  if (this.config['dashboard.https.enabled'] === "true") {
    this.lib = https;
  } else {
    this.lib = http;
  }

  this.config = configuration;
  this.apiKey = configuration.apiKey;
  this.version = configuration.version;
  this.configSet = configuration.configSet;
  this.cookieName = 'continuuity-local-edition';
  this.version = this.version;
  this.secret = 'local-edition-secret';
  this.setCookieSession(this.cookieName, this.secret);
  this.configureExpress();
}

DevServer.prototype.setUpServer = function setUpServer(configuration) {
  this.setAttrs(configuration);
  this.Api.configure(this.config, this.apiKey || null);
  this.start();
}

/**
 * Starts the server after getting config, sets up socket io, configures route handlers.
 */
DevServer.prototype.start = function() {
  this.launchServer();
};

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
    key = this.config['dashboard.ssl.key'];
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

  if (!('dashboard.bind.port' in this.config)) {
    this.config['dashboard.bind.port'] = this.config['dashboard.bind.port.nonssl'];
  }

  this.server.listen(this.config['dashboard.bind.port']);

  this.logger.info('Listening on port', this.config['dashboard.bind.port']);

  /**
  * If mocks are enabled, use mock injector to simulate some responses.
  */
  var enableMocks = !!(argv.enableMocks === 'true');
  if (enableMocks) {
    this.logger.info('Webapp running with mocks enabled.');
    HttpMockInjector = require('../../test/httpMockInjector');
    new HttpMockInjector(nock, this.config['gateway.server.address'], this.config['gateway.server.port']);
  }

}
/**
 * Catch anything uncaught.
 */
process.on('uncaughtException', function (err) {
  debugger;
  devServer.logger.info('Uncaught Exception', err);
});

devServer = new DevServer();
/**
 * Export app.
 */
module.exports = devServer;