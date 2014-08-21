
/**
 * Copyright (c) 2013 Continuuity, Inc.
 */

var util = require('util'),
  fs = require('fs'),
  xml2js = require('xml2js'),
  sys = require('sys'),
  promise = require('q'),
  cluster = require('cluster'),
  os = require('os');

var WebAppServer = require('../common/server');

// The location of continuuity-site.xml
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
  this.getConfig()
    .then(this.setAttrs.bind(this));
};
util.inherits(EntServer, WebAppServer);

EntServer.prototype.setAttrs = function(version) {
  if (this.config['dashboard.https.enabled'] === "true") {
   EntServer.super_.call(this, __dirname, logLevel, true);
  } else {
   EntServer.super_.call(this, __dirname, logLevel, false);
  }
  this.cookieName = 'continuuity-enterprise-edition';
  this.secret = 'enterprise-edition-secret';
  this.logger = this.getLogger('console', 'Enterprise UI');
  this.setCookieSession(this.cookieName, this.secret);
  this.configureExpress();
}

/**
 * Sets config data for application server.
 * @param {Function} opt_callback Callback function to start sever start process.
 */
EntServer.prototype.getConfig = function() {
  var deferredObj = promise.defer();
  fs.readFile(CONF_DIRECTORY + '/cdap-site.xml', function(error, result) {

    if (error) {
      this.logger.error('Could not read configuration file at ' + CONF_DIRECTORY + '/cdap-site.xml');
      return;
    }

    var parser = new xml2js.Parser();
    parser.parseString(result, function(err, result) {
      result = result.configuration.property;
      var localhost = this.getLocalHost();
      for (var item in result) {
        item = result[item];
        this.config[item.name] = item.value[0];
      }
    }.bind(this));
    this.Api.configure(this.config, null);
    this.configSet = true;

    fs.readFile(process.env.COMPONENT_HOME + '/VERSION', "utf-8", function(error, version) {
      deferredObj.resolve(version);
    });
  }.bind(this));
  return deferredObj.promise;
};

/**
 * Starts the server after getting config, sets up socket io, configures route handlers.
 */
EntServer.prototype.start = function() {
  var key,
      cert,
      options = {};

  this.getConfig()
      .then(this.launchServer.bind(this));
};

EntServer.prototype.launchServer = function(version) {
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
   this.server = this.getServerInstance(options, this.app);

   if (!('dashboard.bind.port' in this.config)) {
     this.config['dashboard.bind.port'] = this.config['dashboard.bind.port.ssl.nonssl'];
   }

   var clusters = 'webapp.cluster.count' in this.config ? this.config['webapp.cluster.count'] : 2;

   this.setEnvironment('enterprise', 'Enterprise Reactor', version, this.startServer.bind(this));
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

var entServer = new EntServer();
entServer.start();

/**
 * Catch anything uncaught.
 */
process.on('uncaughtException', function (err) {
  entServer.logger.info('Uncaught Exception', err);
});

/**
 * Export app.
 */
module.exports = entServer;
