
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

var DevServer = function() {
  DevServer.super_.call(this, __dirname, logLevel);

  this.cookieName = 'continuuity-local-edition';
  this.secret = 'local-edition-secret';

  this.logger = this.getLogger();
  this.setVersion();
  this.setCookieSession(this.cookieName, this.secret);
  this.configureExpress();

};
util.inherits(DevServer, WebAppServer);

/**
 * Sets config data for application server.
 * @param {Function} opt_callback Callback function to start sever start process.
 */
DevServer.prototype.getConfig = function(opt_callback) {
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
    fs.readFile(__dirname + '/.credential', "utf-8", function(error, apiKey) {
      self.logger.trace('Configuring with', self.config);
      self.Api.configure(self.config, apiKey || null);
      self.configSet = true;
      if (opt_callback && typeof opt_callback === "function") {
        opt_callback();
      }
    });
  });
};

/**
 * Starts the server after getting config, sets up socket io, configures route handlers.
 */
DevServer.prototype.start = function() {
  this.getConfig(function() {
    this.server = this.getServerInstance(this.app);
    this.io = this.getSocketIo(this.server);
    this.configureIoHandlers(this.io, 'Local', 'developer', this.cookieName, this.secret);
    this.bindRoutes(this.io);
    this.server.listen(this.config['node-port']);
    this.logger.info('Listening on port', this.config['node-port']);
    this.logger.info(this.config);
  }.bind(this));
};


var devServer = new DevServer();
devServer.start();

/**
 * Catch anything uncaught.
 */
process.on('uncaughtException', function (err) {
  devServer.logger.error('Uncaught Exception', err);
});

/**
 * Export app.
 */
module.exports = devServer;
