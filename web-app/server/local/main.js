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

    fs.readFile(__dirname + '/../../../VERSION', "utf-8", function(error, version) {

      fs.readFile(__dirname + '/.credential', "utf-8", function(error, apiKey) {
        self.Api.configure(self.config, apiKey || null);
        self.configSet = true;

        if (typeof opt_callback === "function") {
          opt_callback(version);
        }
      });
    });
  });
};

/**
 * Starts the server after getting config, sets up socket io, configures route handlers.
 */
DevServer.prototype.start = function() {

  this.getConfig(function(version) {

    this.server = this.getServerInstance(this.app);

    this.setEnvironment('local', 'Development Kit', version, function () {

      this.bindRoutes();
      this.server.listen(this.config['dashboard.bind.port']);

      this.logger.info('Listening on port', this.config['dashboard.bind.port']);
      this.logger.info(this.config);

    }.bind(this));

  }.bind(this));

};


var devServer = new DevServer();
devServer.start();

/**
 * Catch anything uncaught.
 */
process.on('uncaughtException', function (err) {
  devServer.logger.info('Uncaught Exception', err);
});

/**
 * Export app.
 */
module.exports = devServer;