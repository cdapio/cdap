/**
 * Copyright © 2013 Cask Data, Inc.
 */

var util = require('util'),
    cluster = require('cluster'),
    os = require('os');
var WebAppServer = require('../common/server');

/**
 * Set environment.
 */
process.env.NODE_ENV = 'production';

/**
 * Log level.
 */
var logLevel = 'INFO';

var EntServer = function() {
  this.productId = 'enterprise';
  this.productName = 'Enterprise CDAP';
  this.cookieName = 'continuuity-enterprise-edition';
  this.secret = 'enterprise-edition-secret';
  EntServer.super_.call(this, __dirname, logLevel, 'Enterprise UI', "enterprise");
};
util.inherits(EntServer, WebAppServer);

EntServer.prototype.startServer = function () {
  this.bindRoutes();
  var self = this;
  this.logger.info('I am the master.', cluster.isMaster);
  var clusters = 'webapp.cluster.count' in this.config ? this.config['webapp.cluster.count'] : 2;
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
    this.server.listen(this.config['dashboard.bind.port']);
  }
  this.logger.info('Listening on port', this.config['dashboard.bind.port']);
}

var entServer = new EntServer();

/**
 * Export app.
 */
module.exports = entServer;
