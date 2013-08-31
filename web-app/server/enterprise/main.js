
/**
 * Copyright (c) 2013 Continuuity, Inc.
 */

var util = require("util"),
  fs = require('fs'),
  xml2js = require('xml2js'),
  sys = require('sys');

var WebAppServer = require('../common/server');

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
  fs.readFile(__dirname + '/continuuity-local.xml', function(error, result) {
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
  this.getConfig(function() {
    this.server = this.getServerInstance(this.app);
    this.io = this.getSocketIo(this.server);
    this.configureIoHandlers(this.io, 'Enterprise', 'developer', this.cookieName, this.secret);
    this.bindRoutes(this.io);
    this.server.listen(this.config['node-port']);
    this.logger.info('Listening on port', this.config['node-port']);
    this.logger.info(this.config);
  }.bind(this));
};


var entServer = new EntServer();
entServer.start();

/**
 * Catch anything uncaught.
 */
process.on('uncaughtException', function (err) {
  entServer.logger.error('Uncaught Exception', err);
});

/**
 * Export app.
 */
module.exports = entServer;