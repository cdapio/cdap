/**
 * Copyright (c) 2013 Continuuity, Inc.
 */

var express = require('express'),
  util = require("util"),
  fs = require('fs'),
  xml2js = require('xml2js'),
  http = require('http'),
  https = require('https'),
  sys = require('sys'),
  io = require('socket.io'),
  Int64 = require('node-int64').Int64,
  log4js = require('log4js'),
  utils = require('connect').utils,
  cookie = require('cookie'),
  crypto = require('crypto'),
  diskspace = require('diskspace');

var WebAppServer = require('../common/server');

// The Continuuity home directory.
var CONTINUUITY_HOME = '/opt/continuuity';
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

var SandboxServer = function () {
  SandboxServer.super_.call(this, __dirname, logLevel);

  this.cookieName = 'continuuity-production-edition';
  this.secret = 'production-edition-secret';

  this.logger = this.getLogger('console', 'Cloud UI');
  this.setEnvironment('sandbox', 'Sandbox Reactor');

  this.setCookieSession(this.cookieName, this.secret);

  // Check SSO.
  this.app.use(this.checkSSO.bind(this));

  // Configure Express.
  this.configureExpress();

};
util.inherits(SandboxServer, WebAppServer);

/**
 * Makes a request to get account information.
 * @param  {string}   path URL of accounts request.
 * @param  {Function} done function to execute upon successful authentication.
 */
SandboxServer.prototype.accountsRequest = function (path, done) {
  var self = this;
  this.logger.info('Requesting from accounts, ', path);

  var options = {
    hostname: self.config['accounts.server.address'],
    port: self.config['accounts.server.port'],
    path: path,
    method: 'GET'
  };

  var req = https.request(options, function (result) {
    result.setEncoding('utf8');
    var data = '';
    result.on('data', function (chunk) {
      data += chunk;
    });
    result.on('end', function () {

      self.logger.info('Response from accounts', result.statusCode, data);

      var status;

      try {
        data = JSON.parse(data);
        status = result.statusCode;
      } catch (e) {
        self.logger.warn('Parsing error', e, data);
        data = e;
        status = 500;
      }

      done(status, data);

    });
  }).on('error', function(e) {
    self.logger.warn(e);
    done(500, e);
  }).end();
};

/**
 * Gets the root of application, used to configure serving files.
 * @return {String} Root of application.
 */
SandboxServer.prototype.getRoot = function () {
  var root;
  if (fs.existsSync(__dirname + '/../client/')) {
    root = __dirname + '/../client/';
  } else {
    root = __dirname + '/../../client/';
  }

  if (process.env.NODE_ENV === 'development') {
    root += 'sandbox';
  }
  return root;
};

/**
 * Renders generic error.
 */
SandboxServer.prototype.renderError = function (req, res) {
  var self = this;
  try {
    res.sendfile('internal-error.html', {'root': self.getRoot()});
  } catch (e) {
    res.write(
      'Internal error. Please email <a href="mailto:support@continuuity.com">' +
      'support@continuuity.com</a>.');
  }

};

/**
 * Renders an access error if SSO fails.
 */
SandboxServer.prototype.renderAccessError = function (req, res) {
  var self = this;
  if (req.session && self.config.info && self.config.info.owner) {
    self.logger.warn('Denied user (current, owner)',
     req.session.account_id, self.config.info.owner.account_id);
  }

  try {
    res.sendfile('access-error.html', {'root': self.getRoot()});
  } catch (e) {
    res.write('Access error.' +
      '<a href="https://accounts.continuuity.com/">Account Home</a>.');
  }

};

/**
 * Sets cookie session for express server.
 * @param {String} cookieName
 * @param {String} secret
 * @override
 */
SandboxServer.prototype.setCookieSession = function (cookieName, secret) {
  this.app.use(express.cookieParser());
  this.app.use(express.session({
    secret: secret,
    key: cookieName,
    cookie: {
      path: '/',
      domain: '.continuuity.net',
      maxAge: 24 * 60 * 60 * 1000
    }
  }));
};

/**
 * Check if SSO is set up.
 */
SandboxServer.prototype.checkSSO = function (req, res, next) {

  if (req.url !== '/') {
    next();
    return;
  }

  var self = this;
  if (req.session.account_id) {

    if (!self.config.info.owner || !self.config.info.owner.account_id) {

      self.logger.error('Checking SSO. Owner information not found in the configuration!');
      self.renderError(req, res);

    } else {
      if (req.session.account_id !== self.config.info.owner.account_id) {
        self.renderAccessError(req, res);
      } else {
        next();
      }
    }

  } else {

    var ret = self.config['gateway.cluster.name'];
    var host = self.config['accounts.server.address'];
    if (self.config['accounts-port'] !== '443') {
      host += ':' + self.config['accounts.server.port'];
    }

    res.redirect('https://' + host + '/sso?return=' + encodeURIComponent(ret));

    self.logger.warn('Redirecting user to authenticate.', host);

  }
};

/**
 * Binds SSO routes for production server. Called upon production server init.
 */
SandboxServer.prototype.bindSSORoutes = function () {

  var self = this;

  // Redirected from central with a fresh nonce.
  // Todo: encrypt an SSO token with the user info.
  this.app.get('/sso/:nonce', function (req, res) {

    var nonce = req.params.nonce;

    self.logger.info('SSO Inbound for nonce', nonce);

    self.accountsRequest('/getSSOUser/' + nonce, function (status, account) {

      if (status !== 200 || account.error) {

        self.logger.warn('getSSOUser', status, account);
        self.logger.warn('SSO Failed. Redirecting to https://' + self.config['accounts.server.address']);
        res.redirect('https://' + self.config['accounts.server.address']);
        res.end();

      } else {

        // Create a unique ID for this session.
        var current_date = (new Date()).valueOf().toString();
        var random = Math.random().toString();
        req.session.session_id = crypto.createHash('sha1')
          .update(current_date + random).digest('hex');

        // Perform ownership check.
        if (process.env.NODE_ENV === 'production') {

          if (!self.config.info.owner || !self.config.info.owner.account_id) {

            self.logger.error('Inbound SSO. Owner information not found in the configuration!');
            self.renderError(req, res);

          } else {

            if (account.account_id !== self.config.info.owner.account_id) {self.renderAccessError(req, res);
            } else {
              req.session.account_id = account.account_id;
              req.session.name = account.first_name + ' ' + account.last_name;
              req.session.api_key = account.api_key;
              res.redirect('/');
            }

          }

        } else {

          req.session.account_id = account.account_id;
          req.session.name = account.first_name + ' ' + account.last_name;
          req.session.api_key = account.api_key;
          res.redirect('/');

        }

      }
    });

  });
};


/**
 * Sets config data for application server.
 * @param {Function} opt_callback Callback function to start sever start process.
 */
SandboxServer.prototype.getConfig = function (opt_callback) {
  var self = this;
  var configPath = __dirname + '/continuuity-local.xml';

  if (fs.existsSync(process.env.CONTINUUITY_HOME + '/conf/continuuity-site.xml')) {
    configPath = process.env.CONTINUUITY_HOME + '/conf/continuuity-site.xml';
  }

  fs.readFile(configPath, function (error, result) {

    var parser = new xml2js.Parser();
    parser.parseString(result, function (err, result) {

      /**
       * Check configuration file error.
       */
      if (err) {
        self.logger.error('Error reading config! Aborting!');
        self.logger.error(err, result);
        process.exit(1);
        return;
      }

      result = result.configuration.property;
      var localhost = self.getLocalHost();
      for (var item in result) {
        item = result[item];
        self.config[item.name] = item.value[0];
      }

      /**
       * Display configuration.
       */
      self.logger.info('Configuring with', self.config);
      self.logger.info('CONTINUUITY_HOME is', process.env.CONTINUUITY_HOME);
      self.logger.info('NODE_ENV is', process.env.NODE_ENV);

      /**
       * Check cluster name.
       */
      if (typeof self.config['gateway.cluster.name'] !== 'string') {
        self.logger.error('No cluster name in config! Aborting!');
        process.exit(1);
        return;
      }
    });

    fs.readFile(CONTINUUITY_HOME + '/VERSION', "utf-8", function(error, version) {

      fs.readFile(__dirname + '/.credential', "utf-8", function (error, apiKey) {
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
SandboxServer.prototype.start = function () {
  var self = this;

  this.getConfig(function (version) {

    /**
     * Request account information and set up environment.
     */
    self.accountsRequest('/api/vpc/getUser/' + self.config['gateway.cluster.name'],
      function (status, info) {

        if (status !== 200 || !info) {
          self.logger.error('No cluster info received from Accounts! Aborting!');
          process.exit(1);
          return;
        }

        self.config.info = info;

        self.setEnvironment('sandbox', 'Sandbox Reactor', version, function (version, address) {

          self.logger.info('Version', version);
          self.logger.info('IP Address', address);

          self.Api.configure(self.config);

          self.bindSSORoutes(); // Goes first to set session.
          self.bindRoutes();

          if (!('cloud-ui-port' in self.config)) {
            self.config['cloud-ui-port'] = DEFAULT_BIND_PORT;
          }

          /**
           * Create an HTTP server that redirects to HTTPS.
           */
          self.server = http.createServer(function (request, response) {

            var host;
            if (request.headers.host) {
              host = request.headers.host.split(':')[0];
            } else {
              host = self.config['gateway.cluster.name'] + '.continuuity.net';
            }

            var path = 'https://' + host + ':' +
              self.config['cloud-ui-ssl-port'] + request.url;

            self.logger.trace('Redirected HTTP to HTTPS');

            response.writeHead(302, {'Location': path});
            response.end();

          }).listen(self.config['cloud-ui-port']);

          /*
           * Don't change this.
           * Reactor start-up script looks for output "Listening on port "
           */
          self.logger.info('Listening on port (HTTP)', self.config['cloud-ui-port']);

          /**
           * HTTPS credentials
           */
          self.certs = {
            ca: fs.readFileSync(__dirname + '/certs/STAR_continuuity_net.ca-bundle'),
            key: fs.readFileSync(__dirname + '/certs/continuuity-com-key.key'),
            cert: fs.readFileSync(__dirname + '/certs/STAR_continuuity_net.crt')
          };

          /**
           * Create the HTTPS server
           */
          self.server = https.createServer(self.certs, self.app).listen(
            self.config['cloud-ui-ssl-port']);

          /*
           * Don't change this.
           * Reactor start-up script looks for output "Listening on port "
           */
          self.logger.info('Listening on port (HTTPS)', self.config['cloud-ui-ssl-port']);

        });
      });
  }.bind(this));
};


var SandboxServer = new SandboxServer();
SandboxServer.start();

/**
 * Catch anything uncaught.
 */
process.on('uncaughtException', function (err) {
  SandboxServer.logger.error('Uncaught Exception', err);
});

/**
 * Export app.
 */
module.exports = SandboxServer;