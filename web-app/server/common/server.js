/**
 * Copyright (c) 2013 Continuuity, Inc.
 * Base server used for developer and enterprise editions. This provides common functionality to
 * set up a node js server and define routes. All custom functionality to an edition
 * must be placed under the server file inside the edition folder.
 */

var express = require('express'),
  Int64 = require('node-int64'),
  fs = require('fs'),
  log4js = require('log4js'),
  http = require('http'),
  https = require('https'),
  cookie = require('cookie'),
  utils = require('connect').utils,
  crypto = require('crypto'),
  path = require('path'),
  request = require('request');

var Api = require('../common/api');
var Env = require('./env');

/**
 * Generic web app server. This is a base class used for creating different editions of the server.
 * This provides base server functionality, logging, and routes setup.
 * @param {string} dirPath from where module is instantiated. This is used becuase __dirname
 * defaults to the location of this module.
 * @param {string} logLevel log level {TRACE|INFO|ERROR}
 * @param {boolean} https whether to use https for requests.
 */
var WebAppServer = function(dirPath, logLevel, https) {
  this.dirPath = dirPath;
  this.LOG_LEVEL = logLevel;
  this.lib = http;
  if (https) {
    this.lib = https;
  }
};

/**
 * Thrift API service.
 */
WebAppServer.prototype.Api = Api;

/**
 * API version.
 */
WebAppServer.prototype.API_VERSION = 'v2';


/**
 * Express app framework.
 */
WebAppServer.prototype.app = express();

/**
 * Config.
 */
WebAppServer.prototype.config = {};

/**
 * Configuration file pulled in and set.
 */
WebAppServer.prototype.configSet = false;

/**
 * Globals
 */
var PRODUCT_VERSION, PRODUCT_ID, PRODUCT_NAME, IP_ADDRESS;

/**
 * Sets version if a version file exists.
 */
WebAppServer.prototype.setEnvironment = function(id, product, version, callback) {

  version = version ? version.replace(/\n/g, '') : 'UNKNOWN';

  PRODUCT_ID = id;
  PRODUCT_NAME = product;
  PRODUCT_VERSION = version;

  this.logger.info('PRODUCT_ID', PRODUCT_ID);
  this.logger.info('PRODUCT_NAME', PRODUCT_NAME);
  this.logger.info('PRODUCT_VERSION', PRODUCT_VERSION);

  Env.getAddress(function (address) {

    IP_ADDRESS = address;
    if (typeof callback === 'function') {
      callback(PRODUCT_VERSION, address);
    }

  }.bind(this));

};

/**
 * Configures logger.
 * @param {string} opt_appenderType log4js appender type.
 * @param {string} opt_logger log4js logger name.
 * @return {Object} instance of logger.
 */
WebAppServer.prototype.getLogger = function(opt_appenderType, opt_loggerName) {
  var appenderType = opt_appenderType || 'console';
  var loggerName = opt_loggerName || 'Developer UI';
  log4js.configure({
    appenders: [
      {type: appenderType}
    ]
  });
  var logger = log4js.getLogger(loggerName);
  logger.setLevel(this.LOG_LEVEL);
  return logger;
};

/**
 * Configures express server.
 */
WebAppServer.prototype.configureExpress = function() {

  this.app.use(express.bodyParser());

  // Workaround to make static files work on cloud.
  if (fs.existsSync(this.dirPath + '/../client/')) {
    this.app.use(express['static'](this.dirPath + '/../client/'));
  } else {
    this.app.use(express['static'](this.dirPath + '/../../client/'));
  }

};

/**
 * Sets a session in express and assigns a cookie identifier.
 * @param {string} cookieName Name of the cookie.
 * @param {string} secret cookie secret.
 */
WebAppServer.prototype.setCookieSession = function(cookieName, secret) {
  this.app.use(express.cookieParser());
  this.app.use(express.session({secret: secret, key: cookieName}));
};

/**
 * Creates http server based on app framework.
 * Currently works only with express.
 * @param {Object} app framework.
 * @return {Object} instance of the http server.
 */
WebAppServer.prototype.getServerInstance = function(app) {
  return this.lib.createServer(app);
};

/**
 * Binds individual expressjs routes. Any additional routes should be added here.
 */
WebAppServer.prototype.bindRoutes = function() {

  var self = this;
  // Check to see if config is set.
  if(!this.configSet) {
    this.logger.info('Configuration file not set ', this.config);
    return false;
  }

  var availableMetrics = {
    'App': [
      { name: 'Events Collected', path: '/reactor/apps/{id}/collect.events' },
      { name: 'Busyness', path: '/reactor/apps/{id}/process.busyness' },
      { name: 'Bytes Stored', path: '/reactor/apps/{id}/store.bytes' },
      { name: 'Queries Served', path: '/reactor/apps/{id}/query.requests' }
    ],
    'Stream': [
      { name: 'Events Collected', path: '/reactor/streams/{id}/collect.events' },
      { name: 'Bytes Collected', path: '/reactor/streams/{id}/collect.bytes' },
      { name: 'Reads per Second', path: '/reactor/streams/{id}/collect.reads' }
    ],
    'Flow': [
      { name: 'Busyness', path: '/reactor/apps/{parent}/flows/{id}/process.busyness' },
      { name: 'Events Processed', path: '/reactor/apps/{parent}/flows/{id}/process.events.processed' },
      { name: 'Bytes Processed', path: '/reactor/apps/{parent}/flows/{id}/process.bytes' },
      { name: 'Errors per Second', path: '/reactor/apps/{parent}/flows/{id}/process.errors' }
    ],
    'Mapreduce': [
      { name: 'Completion', path: '/reactor/apps/{parent}/mapreduce/{id}/process.completion' },
      { name: 'Records Processed', path: '/reactor/apps/{parent}/mapreduce/{id}/process.entries' }
    ],
    'Dataset': [
      { name: 'Bytes per Second', path: '/reactor/datasets/{id}/store.bytes' },
      { name: 'Reads per Second', path: '/reactor/datasets/{id}/store.reads' }
    ],
    'Procedure': [
      { name: 'Requests per Second', path: '/reactor/apps/{parent}/procedures/{id}/query.requests' },
      { name: 'Failures per Second', path: '/reactor/apps/{parent}/procedures/{id}/query.failures' }
    ]

  };

  this.app.get('/rest/metrics/system/:type', function (req, res) {

    var type = req.params.type;
    res.send(availableMetrics[type] || []);

  });

  /**
   * User Metrics Discovery
   */
  this.app.get('/rest/metrics/user/*', function (req, res) {

    var path = req.url.slice(18);

    self.logger.trace('User Metrics', path);

    var options = {
      host: self.config['gateway.server.address'],
      port: self.config['gateway.server.port'],
      method: 'GET',
      path: '/' + self.API_VERSION + '/metrics/available' + path,
      headers: { 'X-Continuuity-ApiKey': req.session ? req.session.api_key : '' }
    };

    var request = self.lib.request(options, function(response) {
      var data = '';
      response.on('data', function (chunk) {
        data += chunk;
      });

      response.on('end', function () {

        try {
          data = JSON.parse(data);
          res.send({ result: data, error: null });
        } catch (e) {
          self.logger.error('Parsing Error', data);
          res.send({ result: null, error: data });
        }

      });
    });

    request.on('error', function(e) {

      res.send({
        result: null,
        error: {
          fatal: 'UserMetricsService: ' + e.code
        }
      });

    });

    request.end();

  });

  /**
   * Enable testing.
   */
  this.app.get('/test', function (req, res) {
    res.sendfile(path.resolve(__dirname + '../../../client/test/TestRunner.html'));
  });

  /*
   * REST DELETE handler.
   */
  this.app.del('/rest/*', function (req, res) {

    var url = self.config['gateway.server.address'] + ':' + self.config['gateway.server.port'];
    var path = url + req.url.replace('/rest', '/' + self.API_VERSION);

    request({
      method: 'DELETE',
      url: 'http://' + path,
      headers: { 'X-Continuuity-ApiKey': req.session ? req.session.api_key : '' }
    }, function (error, response, body) {

      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not DELETE', path, body, error,  response.statusCode);
        if (error && error.code === 'ECONNREFUSED') {
          res.send(500, 'Unable to connect to the Reactor Gateway. Please check your configuration.');
        } else {
          res.send(500, body || error || response.statusCode);
        }
      }
    });

  });

  /*
   * REST PUT handler.
   */
  this.app.put('/rest/*', function (req, res) {
    var url = self.config['gateway.server.address'] + ':' + self.config['gateway.server.port'];
    var path = url + req.url.replace('/rest', '/' + self.API_VERSION);
    var opts = {
      method: 'PUT',
      url: 'http://' + path,
      headers: { 'X-Continuuity-ApiKey': req.session ? req.session.api_key : '' }
    };

    if (req.body) {
      opts.body = req.body.data;
      if (typeof opts.body === 'object') {
        opts.body = JSON.stringify(opts.body);
      }
      opts.body = opts.body || '';
    }

    request(opts, function (error, response, body) {

      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not POST to', path, error || response.statusCode);
        if (error && error.code === 'ECONNREFUSED') {
          res.send(500, 'Unable to connect to the Reactor Gateway. Please check your configuration.');
        } else {
          res.send(500, error || response.statusCode);
        }
      }
    });
  });

  /**
   * Promote handler.
   */
  this.app.post('/rest/apps/:appId/promote', function (req, res) {
    var url = self.config['gateway.server.address'] + ':' + self.config['gateway.server.port'];
    var path = url + req.url.replace('/rest', '/' + self.API_VERSION);
    var opts = {
      method: 'POST',
      url: 'http://' + path,
      headers: { 'X-Continuuity-ApiKey': req.session ? req.session.api_key : '' }
    };

    if (req.body) {
      opts.body = {
        hostname: req.body.hostname
      };
      opts.body = JSON.stringify(opts.body) || '';
      opts.headers = {
        'X-Continuuity-ApiKey': req.body.apiKey
      };
    }

    request(opts, function (error, response, body) {

      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not POST to', path, error || response.statusCode);
        if (error && error.code === 'ECONNREFUSED') {
          res.send(500, 'Unable to connect to the Reactor Gateway. Please check your configuration.');
        } else {
          res.send(500, error || response.statusCode);
        }
      }
    });
  });

  /*
   * REST POST handler.
   */
  this.app.post('/rest/*', function (req, res) {
    var url = self.config['gateway.server.address'] + ':' + self.config['gateway.server.port'];
    var path = url + req.url.replace('/rest', '/' + self.API_VERSION);
    var opts = {
      method: 'POST',
      url: 'http://' + path,
      headers: { 'X-Continuuity-ApiKey': req.session ? req.session.api_key : '' }
    };

    if (req.body) {
      opts.body = req.body.data;
      if (typeof opts.body === 'object') {
        opts.body = JSON.stringify(opts.body);
      }
      opts.body = opts.body || '';
    }

    request(opts, function (error, response, body) {

      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not POST to', path, error || response.statusCode);
        if (error && error.code === 'ECONNREFUSED') {
          res.send(500, 'Unable to connect to the Reactor Gateway. Please check your configuration.');
        } else {
          res.send(500, error || response.statusCode);
        }
      }
    });
  });

  /*
   * REST GET handler.
   */
  this.app.get('/rest/*', function (req, res) {

    var url = self.config['gateway.server.address'] + ':' + self.config['gateway.server.port'];
    var path = url + req.url.replace('/rest', '/' + self.API_VERSION);

    var opts = {
      method: 'GET',
      url: 'http://' + path,
      headers: { 'X-Continuuity-ApiKey': req.session ? req.session.api_key : '' }
    };

    request(opts, function (error, response, body) {

      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not GET', path, error || response.statusCode);
        if (error && error.code === 'ECONNREFUSED') {
          res.send(500, 'Unable to connect to the Reactor Gateway. Please check your configuration.');
        } else {
          res.send(500, error || response.statusCode);
        }
      }
    });
  });

  /*
   * Metrics Handler
   */
  this.app.post('/metrics', function (req, res) {

    var pathList = req.body;
    var accountID = 'developer';

    self.logger.trace('Metrics ', pathList);

    if (!pathList) {
      self.logger.error('No paths posted to Metrics.');
      res.send({
        result: null,
        error: {
          fatal: 'MetricsService: No paths provided.'
        }
      });
      return;
    }

    var content = JSON.stringify(pathList);

    var options = {
      host: self.config['gateway.server.address'],
      port: self.config['gateway.server.port'],
      path: '/' + self.API_VERSION + '/metrics',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': content.length,
        'X-Continuuity-ApiKey': req.session ? req.session.api_key : ''
      }
    };

    var request = self.lib.request(options, function(response) {
      var data = '';
      response.on('data', function (chunk) {
        data += chunk;
      });

      response.on('end', function () {

        try {
          data = JSON.parse(data);
          res.send({ result: data, error: null });
        } catch (e) {
          self.logger.error('Parsing Error', data);
          res.send({ result: null, error: 'Parsing Error' });
        }

      });
    });

    request.on('error', function(e) {

      res.send({
        result: null,
        error: {
          fatal: 'MetricsService: ' + e.code
        }
      });

    });

    request.write(content);
    request.end();

  });

  /**
   * Upload an Application archive.
   */
  this.app.post('/upload/:file', function (req, res) {
    var url = 'http://' + self.config['gateway.server.address'] + ':' +
      self.config['gateway.server.port'] + '/' + self.API_VERSION + '/apps';

    var opts = {
      method: 'POST',
      url: url,
      headers: { 'X-Continuuity-ApiKey': req.session ? req.session.api_key : '' }
    };

    var x = request.post(opts);
    req.pipe(x);
    x.pipe(res);
  });

  this.app.get('/upload/status', function (req, res) {

    var options = {
      host: self.config['gateway.server.address'],
      port: self.config['gateway.server.port'],
      path: '/' + self.API_VERSION + '/deploy/status',
      method: 'GET',
      headers: { 'X-Continuuity-ApiKey': req.session ? req.session.api_key : '' }
    };

    var request = self.lib.request(options, function (response) {

      var data = '';
      response.on('data', function (chunk) {
        data += chunk;
      });

      response.on('end', function () {

        if (response.statusCode !== 200) {
          res.send(200, 'Upload error: ' + data);
          self.logger.error('Could not upload file ' + req.params.file, data);
        } else {
          res.send(JSON.parse(data));
        }

      });

    });

    request.end();

  });

  this.app.post('/unrecoverable/reset', function (req, res) {

    var host = self.config['gateway.server.address'] + ':' + self.config['gateway.server.port'];

    var opts = {
      method: 'POST',
      url: 'http://' + host + '/' + self.API_VERSION + '/unrecoverable/reset',
      headers: { 'X-Continuuity-ApiKey': req.session ? req.session.api_key : '' }
    };

    request(opts, function (error, response, body) {

      if (error || response.statusCode !== 200) {
        res.send(400, body);
      } else {
        res.send('OK');
      }

    });

  });

  this.app.get('/environment', function (req, res) {

    var environment = {
      'product_id': PRODUCT_ID,
      'product_name': PRODUCT_NAME,
      'product_version': PRODUCT_VERSION,
      'ip': IP_ADDRESS
    };

    if (req.session.account_id) {

      environment.account = {
        account_id: req.session.account_id,
        name: req.session.name
      };

    }

    if (process.env.NODE_ENV !== 'production') {
      environment.credential = self.Api.credential;

    } else {
      if ('info' in self.config) {
        environment.cluster = self.config.info;
      }
    }

    res.send(environment);

  });

  /**
   * Check for new version.
   * http://www.continuuity.com/version
   */
  this.app.get('/version', function (req, res) {
    var options = {
      host: 'www.continuuity.com',
      path: '/version',
      port: '80'
    };

    var request = self.lib.request(options, function(response) {
      var data = '';
      response.on('data', function (chunk) {
        data += chunk;
      });

      response.on('end', function () {

        data = data.replace(/\n/g, '');

        res.send({
          current: PRODUCT_VERSION,
          newest: data
        });

      });
    });

    request.end();

  });

  /**
   * Get a list of push destinations.
   */
  this.app.get('/destinations', function  (req, res) {

    fs.readFile(self.dirPath + '/.credential', 'utf-8', function (error, result) {

      res.on('error', function (e) {
        self.logger.trace('/destinations', e);
      });

      if (error) {

        res.write('false');
        res.end();

      } else {

        var options = {
          host: self.config['accounts.server.address'],
          path: '/api/vpc/list/' + result,
          port: self.config['accounts.server.port']
        };

        var request = https.request(options, function(response) {

          var data = '';
          response.on('data', function (chunk) {
            data += chunk;
          });

          response.on('end', function () {
            res.write(data);
            res.end();
          });

          response.on('error', function () {
            res.write('network');
            res.end();
          });
        });

        request.on('error', function () {
          res.write('network');
          res.end();
        });

        request.on('socket', function (socket) {
          socket.setTimeout(10000);
          socket.on('timeout', function() {

            request.abort();
            res.write('network');
            res.end();

          });
        });
        request.end();
      }
    });
  });

  /**
   * Save a credential / API Key.
   */
  this.app.post('/credential', function (req, res) {

    var apiKey = req.body.apiKey;

    // Write credentials to file.
    fs.writeFile(self.dirPath + '/.credential', apiKey,
      function (error, result) {
        if (error) {

          self.logger.warn('Could not write to ./.credential', error, result);
          res.write('Error: Could not write credentials file.');
          res.end();

        } else {
          self.Api.credential = apiKey;

          res.write('true');
          res.end();

        }
    });
  });

  /**
   * Catch port binding errors.
   */
  this.app.on('error', function () {
    self.logger.warn('Port ' + self.config['dashboard.bind.port'] + ' is in use.');
    process.exit(1);
  });
};

/**
 * Gets the local host.
 * @return {string} localhost ip address.
 */
WebAppServer.prototype.getLocalHost = function() {
  var os = require('os');
  var ifaces = os.networkInterfaces();
  var localhost = '';

  for (var dev in ifaces) {
    for (var i = 0, len = ifaces[dev].length; i < len; i++) {
      var details = ifaces[dev][i];
      if (details.family === 'IPv4') {
        if (dev === 'lo0') {
          localhost = details.address;
          break;
        }
      }
    }
  }
  return localhost;
};

/**
 * Export app.
 */
module.exports = WebAppServer;


