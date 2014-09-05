
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

/**
 * Set environment.
 */
process.env.NODE_ENV = 'production';

/**
 * Log level.
 */
var logLevel = 'INFO';

var EntServer = function() {
  EntServer.super_.call(this, __dirname, logLevel, 'Enterprise UI');
  this.extractConfig()
      .then(function () {
        this.setUpServer();
      }.bind(this));
};
util.inherits(EntServer, WebAppServer);


EntServer.prototype.extractConfig = configParser.extractConfig;

EntServer.prototype.setUpServer = function setUpServer(configuration) {
  this.setAttrs();
  this.Api.configure(this.config, this.apiKey || null);
  this.launchServer();
}
EntServer.prototype.setAttrs = function() {
  if (this.config['dashboard.https.enabled'] === "true") {
    this.lib = https;
  } else {
    this.lib = http;
  }
  this.apiKey = this.config.apiKey;
  this.version = this.config.version;
  this.configSet = this.config.configSet;
  this.cookieName = 'continuuity-enterprise-edition';
  this.secret = 'enterprise-edition-secret';
  this.setCookieSession(this.cookieName, this.secret);
  this.configureExpress();
}


EntServer.prototype.launchServer = function() {
   var key,
       cert,
       options = this.configureSSL() || {};
   this.server = this.getServerInstance(options, this.app);
   //LaunchServer and then StartServer?? Kind of redundant on names. Any alternative is welcome.
   this.setEnvironment('enterprise', 'Enterprise Reactor', this.version, this.startServer.bind(this));
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
