/**
 * Copyright (c) 2013 Continuuity, Inc.
 */

var util = require("util"),
  fs = require('fs'),
  xml2js = require('xml2js'),
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

var DevServer = function() {
  this.getConfig()
      .then(this.setAttrs.bind(this));
};
util.inherits(DevServer, WebAppServer);

DevServer.prototype.setAttrs = function(version) {
  if (this.config['dashboard.https.enabled'] === "true") {
    DevServer.super_.call(this, __dirname, logLevel, true);
  } else {
    DevServer.super_.call(this, __dirname, logLevel, false);
  }
  this.cookieName = 'continuuity-local-edition';
  this.secret = 'local-edition-secret';
  this.logger = this.getLogger();
  this.setCookieSession(this.cookieName, this.secret);
  this.configureExpress();
}

/**
 * Sets config data for application server.
 * @param {Function} opt_callback Callback function to start sever start process.
 */
DevServer.prototype.getConfig = function() {
  var deferredObj = promise.defer();
//      promises = [],
//      readfile;

//  readfile = function (path, format) {
//    var deferred = promise.defer();
//    if (format) {
//      fs.readFile(path, format, function(err, result) {
//        deferred.resolve(result);
//      });
//    } else {
//      fs.readFile(path, function(err, result) {
//        deferred.resolve(result);
//      });
//    }
//    return deferred.promise;
//  };
//  //There should definitely be better way to do this.
//  readfile(__dirname + '/continuuity-local.xml')
//    .then( function(result, error) {
//      var parser = new xml2js.Parser();
//      parser.parseString(result, function(err, result) {
//        result = result.configuration.property;
//        for (var item in result) {
//          item = result[item];
//          this.config[item.name] = item.value[0];
//        }
//      }.bind(this));
//      return readfile(__dirname + '/../../../VERSION', 'utf-8');
//    }.bind(this))
//    .then( function(version) {
//      return readfile(__dirname + '/.credential', 'utf-8');
//    })
//    .then( function(error, apiKey) {
//      this.Api.configure(this.config, apiKey || null);
//      this.configSet = true;
//      deferredObj.resolve(version);
//    }.bind(this));

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
          deferredObj.resolve(version);
        });
      });
    });

  return deferredObj.promise;
};

/**
 * Starts the server after getting config, sets up socket io, configures route handlers.
 */
DevServer.prototype.start = function() {
  this.getConfig()
      .then(this.launchServer.bind(this));
};

DevServer.prototype.launchServer = function(version) {
   var key,
       cert,
       options = {};
   options = this.configureSSL();
   this.server = this.getServerInstance(options, this.app);
   this.setEnvironment('local', 'Development Kit', version, this.startServer.bind(this));
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
  this.logger.info(this.config);

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

var devServer = new DevServer();
devServer.start();


/**
 * Export app.
 */
module.exports = devServer;