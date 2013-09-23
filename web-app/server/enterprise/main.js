
/**
 * Copyright (c) 2013 Continuuity, Inc.
 */

var util = require("util"),
  fs = require('fs'),
  xml2js = require('xml2js'),
  sys = require('sys'),
  cluster = require('cluster'),
  os = require('os');

var WebAppServer = require('../common/server');

// The location of continuuity-site.xml
var CONF_DIRECTORY = '/etc/continuuity/conf';

/**
 * Set environment.
 */
process.env.NODE_ENV = 'development';

/**
 * Log level.
 */
var logLevel = 'INFO';

var EntServer = function() {
  EntServer.super_.call(this, __dirname, logLevel);

  this.cookieName = 'continuuity-enterprise-edition';
  this.secret = 'enterprise-edition-secret';

  this.logger = this.getLogger('console', 'Enterprise UI');
  this.setVersion();
  this.setCookieSession(this.cookieName, this.secret);
  this.configureExpress();
  this.setCookieSession('continuuity-enterprise-edition', this.secret);

};
util.inherits(EntServer, WebAppServer);

/**
 * Sets config data for application server.
 * @param {Function} opt_callback Callback function to start sever start process.
 */
EntServer.prototype.getConfig = function(opt_callback) {
  var self = this;
  fs.readFile(CONF_DIRECTORY + '/continuuity-site.xml', function(error, result) {
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
    if (opt_callback && typeof opt_callback === "function") {
      opt_callback();
    }
  });
};

/**
 * Starts the server after getting config, sets up socket io, configures route handlers.
 */
EntServer.prototype.start = function() {
  var self = this;
  self.getConfig(function() {
    self.server = self.getServerInstance(self.app);
    self.io = self.getSocketIo(self.server);
    self.configureIoHandlers(self.io, 'Enterprise', 'developer', self.cookieName, self.secret);
    self.bindRoutes(self.io);

    var clusters = 'webapp.cluster.count' in self.config ? self.config['webapp.cluster.count'] : 2;

    self.logger.info('Is cluster master? ', cluster.isMaster);
    if (cluster.isMaster) {
      for (var i = 0; i < clusters; i++) {
        cluster.fork();
      }

      cluster.on('online', function (worker) {
        self.logger.info('worker ' + worker.id + ' was forked.');
      });

      cluster.on('listening', function (worker, address) {
        self.logger.info('worker ' + worker.id + ' is listening on ' + address.address + ':' +
          address.port);
      });

      cluster.on('exit', function (worker, code, signal) {
        self.logger.info('worker ' + worker.process.pid + ' died.');
      });

    } else {
      self.server.listen(self.config['dashboard.bind.port']);
    }
    self.logger.info('Listening on port', self.config['dashboard.bind.port']);
    self.logger.info(self.config);
  });
};


var entServer = new EntServer();
entServer.start();

/**
 * Catch anything uncaught.
 */
process.on('uncaughtException', function (err) {
//  entServer.logger.info('Uncaught Exception', err);
});

/**
 * Export app.
 */
module.exports = entServer;