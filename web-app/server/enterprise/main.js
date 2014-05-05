
/**
 * Copyright (c) 2013 Continuuity, Inc.
 */

var util = require('util'),
  fs = require('fs'),
  xml2js = require('xml2js'),
  sys = require('sys'),
  cluster = require('cluster'),
  os = require('os');

var WebAppServer = require('../common/server');

// The location of continuuity-site.xml
var CONF_DIRECTORY = '/etc/continuuity/conf';
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

  this.cookieName = 'continuuity-enterprise-edition';
  this.secret = 'enterprise-edition-secret';
  this.logger = this.getLogger('console', 'Enterprise UI');
  this.setCookieSession(this.cookieName, this.secret);
  this.configureExpress();

};
util.inherits(EntServer, WebAppServer);

/**
 * Sets config data for application server.
 * @param {Function} opt_callback Callback function to start sever start process.
 */
EntServer.prototype.getConfig = function(opt_callback) {
  var self = this;
  fs.readFile(CONF_DIRECTORY + '/continuuity-site.xml', function(error, result) {

    if (error) {
      self.logger.error('Could not read configuration file at ' + CONF_DIRECTORY + '/continuuity-site.xml');
      return;
    }

    var parser = new xml2js.Parser();
    parser.parseString(result, function(err, result) {
      result = result.configuration.property;
      var localhost = self.getLocalHost();
      for (var item in result) {
        item = result[item];
        self.config[item.name] = item.value[0];
      }
    });
    self.Api.configure(self.config, null);
    self.configSet = true;

    fs.readFile(process.env.COMPONENT_HOME + '/VERSION', "utf-8", function(error, version) {

      if (typeof opt_callback === "function") {
        opt_callback(version);
      }

    });
  });
};

/**
 * Starts the server after getting config, sets up socket io, configures route handlers.
 */
EntServer.prototype.start = function() {
  var self = this;

  self.getConfig(function(version) {

    self.server = self.getServerInstance(self.app);

    if (!('dashboard.bind.port' in self.config)) {
      self.config['dashboard.bind.port'] = DEFAULT_BIND_PORT;
    }

    var clusters = 'webapp.cluster.count' in self.config ? self.config['webapp.cluster.count'] : 2;

    self.setEnvironment('enterprise', 'Enterprise Reactor', version, function () {

      self.bindRoutes();
      self.logger.info('I am the master.', cluster.isMaster);

      if (cluster.isMaster) {
        for (var i = 0; i < clusters; i++) {
          cluster.fork();
        }

        cluster.on('online', function (worker) {
          self.logger.info('Worker ' + worker.id + ' was forked with pid ' + worker.process.pid);
        });

        cluster.on('listening', function (worker, address) {
          self.logger.info('Worker ' + worker.id + ' with pid ' + worker.process.pid +
          ' is listening on ' + address.address + ':' + address.port);
        });

        cluster.on('exit', function (worker, code, signal) {
          self.logger.info('Worker ' + worker.process.pid + ' died.');

          // Create a new process once one dies.
          var newWorker = cluster.fork();
          self.logger.info('Started new worker at ' + newWorker.process.pid);
        });

      } else {
        self.server.listen(self.config['dashboard.bind.port']);
      }

      self.logger.info('Listening on port', self.config['dashboard.bind.port']);
      self.logger.info(self.config);

    }.bind(this));

  });
};


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
