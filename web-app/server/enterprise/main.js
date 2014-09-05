
/**
 * Copyright (c) 2013 Cask Data, Inc.
 */

var util = require('util'),
  fs = require('fs'),
  http = require('http'),
  https = require('https'),
  promise = require('q'),
  cluster = require('cluster'),
  lodash = require('lodash'),
  os = require('os');

var WebAppServer = require('../common/server'),
    configParser = require('../common/configParser');

var CONF_DIRECTORY = '/etc/cdap/conf';
// Default port for the Dashboard.
var DEFAULT_BIND_PORT = 9999;

/**
 * Set environment.
 */
process.env.NODE_ENV = 'production';

/**
 * Log level.
 */
var logLevel = 'INFO';

var EntServer = function() {
  EntServer.super_.call(this, __dirname, logLevel);
  this.extractBaseConfig("enterprise")
      .then(function onBaseConfigExtract() {
        return this.getConfig(CONF_DIRECTORY + '/cdap-site.xml');
      }.bind(this))
      .then(function (configuration) {
        console.log(configuration);
        this.config = configuration;
        this.setUpServer(configuration);
      }.bind(this));
};
util.inherits(EntServer, WebAppServer);

EntServer.prototype.extractBaseConfig = configParser.extractBaseConfig;

EntServer.prototype.extractConfigFromXml = configParser.extractConfigFromXml;

EntServer.prototype.getConfig = function getConfig(filename) {
  var deferred = promise.defer(),
      configObj = {};
  this.extractConfigFromXml(filename)
    .then(function onExtractConfigFromXml(configuration) {
      configObj = lodash.extend(this.baseConfig, configuration);
      deferred.resolve(configObj);
    }.bind(this));
  return deferred.promise;
}

EntServer.prototype.setUpServer = function setUpServer(configuration) {
  this.setAttrs(configuration);
  this.Api.configure(this.config, this.apiKey || null);
  this.start();
}

EntServer.prototype.setAttrs = function(configuration) {
  if (this.config['dashboard.https.enabled'] === "true") {
    this.lib = https;
  } else {
    this.lib = http;
  }
  this.apiKey = configuration.apiKey;
  this.version = configuration.version;
  this.configSet = configuration.configSet;
  this.cookieName = 'continuuity-enterprise-edition';
  this.secret = 'enterprise-edition-secret';
  this.logger = this.getLogger('console', 'Enterprise UI');
  this.setCookieSession(this.cookieName, this.secret);
  this.configureExpress();
}

/**
 * Starts the server after getting config, sets up socket io, configures route handlers.
 */
EntServer.prototype.start = function() {
  this.launchServer();
};

EntServer.prototype.launchServer = function() {
   var key,
       cert,
       options = this.configureSSL() || {};
   this.server = this.getServerInstance(options, this.app);
   //LaunchServer and then StartServer?? Kind of redundant on names. Any alternative is welcome.
   this.setEnvironment('local', 'Development Kit', this.version, this.startServer.bind(this));
}

EntServer.prototype.configureSSL = function () {
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

EntServer.prototype.startServer = function () {
 this.bindRoutes();
 this.logger.info('I am the master.', cluster.isMaster);

 if (cluster.isMaster) {
   for (var i = 0; i < clusters; i++) {
     cluster.fork();
   }

   cluster.on('online', function (worker) {
     this.logger.info('Worker ' + worker.id + ' was forked with pid ' + worker.process.pid);
   });

   cluster.on('listening', function (worker, address) {
     this.logger.info('Worker ' + worker.id + ' with pid ' + worker.process.pid +
     ' is listening on ' + address.address + ':' + address.port);
   });

   cluster.on('exit', function (worker, code, signal) {
     this.logger.info('Worker ' + worker.process.pid + ' died.');

     // Create a new process once one dies.
     var newWorker = cluster.fork();
     this.logger.info('Started new worker at ' + newWorker.process.pid);
   });

 } else {
   this.server.listen(this.config['dashboard.bind.port']);
 }

 this.logger.info('Listening on port', this.config['dashboard.bind.port']);
 this.logger.info(this.config);

}

/**
 * Catch anything uncaught.
 */
process.on('uncaughtException', function (err) {
  entServer.logger.info('Uncaught Exception', err);
});

var entServer = new EntServer();


/**
 * Export app.
 */
module.exports = entServer;
