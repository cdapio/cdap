/**
 * Copyright (c) 2013 Continuuity, Inc.
 * Base server used for developer and enterprise editions. This provides common functionality to
 * set up a node js server with socket io and define routes. All custom functionality to an edition
 * must be placed under the server file inside the edition folder.
 */

var express = require('express'),
  io = require('socket.io'),
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

/**
 * Generic web app server. This is a base class used for creating different editions of the server.
 * This provides base server functionality, logging, routes and socket io setup.
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
 * Server version.
 */
WebAppServer.prototype.VERSION = '';

/**
 * API version.
 */
WebAppServer.prototype.API_VERSION = 'v2';


/**
 * Express app framework.
 */
WebAppServer.prototype.app = express();

/**
 * Socket io.
 */
WebAppServer.prototype.io = {};

/**
 * Socket to listen to emit events and data.
 */
WebAppServer.prototype.socket = null;

/**
 * Config.
 */
WebAppServer.prototype.config = {};

/**
 * Configuration file pulled in and set.
 */
WebAppServer.prototype.configSet = false;

/**
 * Sets version if a version file exists.
 */
WebAppServer.prototype.setVersion = function() {
  try {
    this.VERSION = fs.readFileSync(this.dirPath + '../../../VERSION', 'utf8');
  } catch (e) {
    this.VERSION = 'UNKNOWN';
  }
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
 * Opens an io socket using the server.
 * @param {Object} Http server used by application.
 * @return {Object} instane of io socket listening to server.
 */
WebAppServer.prototype.getSocketIo = function (server, environment, certs) {
  var io;
  if (certs) {
    io = require('socket.io').listen(server, certs);
  } else {
    io = require('socket.io').listen(server);
  }
  io.configure(environment || 'development', function () {
    io.enable('browser client minification');
    io.enable('browser client gzip');
    io.set('transports', ['websocket', 'xhr-polling']);
    io.set('log level', 1);
    io.set('resource', '/socket.io');
  });
  return io;
};

/**
 * Defines actions in response to a recieving data from a socket.
 * @param {Object} io socket io manager.
 * @param {Object} request a socket request.
 * @param {Object} error error.
 * @param {Object} response for hte socket request.
 */
WebAppServer.prototype.socketResponse = function(io, request, error, response) {
  // Emit to all open socket connections, this enables multi broswer tab support.
  io.sockets.emit('exec', error, {
    method: request.method,
    params: typeof response === 'string' ? JSON.parse(response) : response,
    id: request.id
  });
};

/**
 * Configures socket io handlers. Async binds socket io methods.
 * @param {Object} instance of the socket io.
 * @param {string} product of evn for socket to emit.
 * @param {string} version.
 */
WebAppServer.prototype.configureIoHandlers = function(
  io, product, version, cookieName, secret, location) {
  var self = this;
  var sockets = [];

  //Authorize and accept socket connection only if cookie exists.
  io.set('authorization', function (data, accept) {

    if (data.headers.cookie) {

      var cookies = cookie.parse(data.headers.cookie);
      var signedCookies = utils.parseSignedCookies(cookies, secret);
      var obj = utils.parseJSONCookies(signedCookies);
      data.session_id = obj[cookieName];
      if ('continuuity-sso' in obj) {
        if ('api_key' in obj['continuuity-sso']) {
          data.api_key = obj['continuuity-sso'].api_key;
        }

        if ('account_id' in obj['continuuity-sso']) {
          data.account_id = obj['continuuity-sso'].account_id;
        }

        if ('name' in obj['continuuity-sso']) {
          data.name = obj['continuuity-sso'].name;
        }
      }


    } else {

      return accept('No cookie transmitted', false);

    }

    accept(null, true);
  });

  io.sockets.on('connection', function (newSocket) {

    sockets.push(newSocket);

    // Join room based on session id.
    newSocket.join(newSocket.handshake.session_id);

    var ip = '';

    if (typeof Env !== 'undefined') {
      if ('ip' in Env) {
        ip = Env.ip;
      }
    }
    var envVars = {
      'product': product,
      'location': location || '',
      'version': version || 'UNKNOWN',
      'ip': ip
    };
    if (process.env.NODE_ENV !== 'production') {
      envVars.credential = self.Api.credential;
    } else {
      if ('info' in self.config) {
        envVars.cluster = self.config.info;
      }
      if ('handshake' in newSocket) {
        envVars.account = {
          'account_id': newSocket.handshake.account_id,
          'name': newSocket.handshake.name
        };
      }
    }

    newSocket.emit('env', envVars);

    newSocket.on('metadata', function (request) {
      self.Api.metadata(version, request.method, request.params, function (error, response) {
        self.socketResponse(io, request, error, response);
      });
    });

    newSocket.on('far', function (request) {
      self.Api.far(version, request.method, request.params, function (error, response) {
        self.socketResponse(io, request, error, response);
      });
    });

    newSocket.on('gateway', function (request) {
      self.Api.gateway('apikey', request.method, request.params, function (error, response) {
        self.socketResponse(io, request, error, response);
      });
    });

    newSocket.on('monitor', function (request) {
      self.Api.monitor(version, request.method, request.params, function (error, response) {
        self.socketResponse(io, request, error, response);
      });
    });

    newSocket.on('manager', function (request) {
      self.Api.manager(version, request.method, request.params, function (error, response) {

        if (response && response.length) {
          var int64values = {
            'lastStarted': 1,
            'lastStopped': 1,
            'startTime': 1,
            'endTime': 1
          };
          for (var i = 0; i < response.length; i ++) {
            for (var j in response[i]) {
              if (j in int64values) {
                response[i][j] = parseInt(response[i][j].toString(), 10);
              }
            }
          }
        }
        self.socketResponse(io, request, error, response);
      });
    });
  });
};

/**
 * Binds individual expressjs routes. Any additional routes should be added here.
 * @param {Object} io socket io adapter.
 */
WebAppServer.prototype.bindRoutes = function(io) {
  var self = this;
  // Check to see if config is set.
  if(!this.configSet) {
    this.logger.info('Configuration file not set ', this.config);
    return false;
  }

  var availableMetrics = {
    'App': [
      { name: 'Events Collected', path: '/collect/events/apps/{id}' },
      { name: 'Busyness', path: '/process/busyness/{id}' },
      { name: 'Bytes Stored', path: '/store/bytes/apps/{id}' },
      { name: 'Queries Served', path: '/query/requests/{id}' }
    ],
    'Stream': [
      { name: 'Events Collected', path: '/collect/events/streams/{id}' },
      { name: 'Bytes Collected', path: '/collect/bytes/streams/{id}' },
      { name: 'Reads per Second', path: '/collect/reads/streams/{id}' }
    ],
    'Flow': [
      { name: 'Busyness', path: '/process/busyness/{parent}/flows/{id}' },
      { name: 'Events Processed', path: '/process/events/{parent}/flows/{id}' },
      { name: 'Bytes Processed', path: '/process/bytes/{parent}/flows/{id}' },
      { name: 'Errors per Second', path: '/process/errors/{parent}/flows/{id}' }
    ],
    'Batch': [
      { name: 'Completion', path: '/process/completion/{parent}/mapreduce/{id}' },
      { name: 'Entries Processed', path: '/process/entries/{parent}/mapreduce/{id}' }
    ],
    'Dataset': [
      { name: 'Bytes per Second', path: '/store/bytes/datasets/{id}' },
      { name: 'Reads per Second', path: '/store/reads/datasets/{id}' }
    ],
    'Procedure': [
      { name: 'Requests per Second', path: '/query/requests/{parent}/procedures/{id}' },
      { name: 'Failures per Second', path: '/query/failures/{parent}/procedures/{id}' }
    ]

  };

  this.app.get('/rest/metrics/system/:type', function (req, res) {

    var type = req.params.type;
    res.send(availableMetrics[type]);

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
      path: '/metrics/available' + path,
      method: 'GET'
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
    request.del('http://' + path, function (error, response, body) {

      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not DELETE', path, error || response.statusCode);
        if (error.code === 'ECONNREFUSED') {
          res.send(500, 'Unable to connect to the Reactor Gateway. Please check your configuration.');
        } else {
          res.send(500, error || response.statusCode);
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
    request.put('http://' + path, function (error, response, body) {

      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not PUT to', path, error || response.statusCode);
        if (error.code === 'ECONNREFUSED') {
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
    var opts = {url: 'http://' + path};
    if (req.body) {
      opts.body = req.body.data;
      if (typeof opts.body === 'object') {
        opts.body = JSON.stringify(opts.body);
      }
    }

    request.post(opts, function (error, response, body) {

      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not POST to', path, error || response.statusCode);
        if (error.code === 'ECONNREFUSED') {
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

    request('http://' + path, function (error, response, body) {



      if (!error && response.statusCode === 200) {
        res.send(body);
      } else {
        self.logger.error('Could not GET', path, error || response.statusCode);
        if (error.code === 'ECONNREFUSED') {
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

    var content = JSON.stringify(pathList);

    var options = {
      host: self.config['gateway.server.address'],
      port: self.config['gateway.server.port'],
      path: '/metrics',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': content.length
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

    var length = req.header('Content-length');
    var location = '/tmp/' + req.params.file;
    var data = new Buffer(parseInt(length, 10));
    var idx = 0;
    req.on('data', function(raw) {
      raw.copy(data, idx);
      idx += raw.length;
    });

    req.on('end', function() {
      fs.writeFile(location, data, function(err) {
        if(err) {
          res.send(err);
        } else {
          var options = {
            host: self.config['gateway.server.address'],
            port: self.config['gateway.server.port'],
            path: '/' + self.API_VERSION + '/apps',
            method: 'PUT',
            headers: {
              'Content-length': length,
              'X-Archive-Name': req.params.file,
              'Transfer-Encoding': 'chunked'
            }
          };
          var request = self.lib.request(options, function (response) {
            if (response.statusCode !== 200) {
              res.send(400, 'Could not upload file.');
              self.logger.error('Could not upload file ' + req.params.file);
            } else {
              res.send('OK');
            }
          });

          request.on('error', function(e) {

          });
          var stream = fs.createReadStream(location);
          stream.on('data', function(chunk) {
            request.write(chunk);
          });
          stream.on('end', function() {
            request.end();
          });

        }
      });
    });
  });

  this.app.post('/unrecoverable/reset', function (req, res) {

    var host = self.config['gateway.server.address'] + ':' + self.config['gateway.server.port'];
    var opts = { url: 'http://' + host + '/' + self.API_VERSION + '/unrecoverable/reset' };

    request.del(opts, function (error, response, body) {

      if (error || response.statusCode !== 200) {
        res.send(400, body);
      } else {
        res.send('OK');
      }

    });

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
          current: self.VERSION,
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
          host: self.config['accounts-host'],
          path: '/api/vpc/list/' + result,
          port: self.config['accounts-port']
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


