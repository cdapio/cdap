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
  path = require('path');

var Api = require('../common/api');

/**
 * Generic web app server. This is a base class used for creating different editions of the server.
 * This provides base server functionality, logging, routes and socket io setup.
 * @param {string} dirPath from where module is instantiated. This is used becuase __dirname defaults
 *    to the location of this module.
 * @param {string} logLevel log level {TRACE|INFO|ERROR}
 */
var WebAppServer = function(dirPath, logLevel) {
  this.dirPath = dirPath;
  this.LOG_LEVEL = logLevel;
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
  return http.createServer(app);
};

/**
 * Opens an io socket using the server.
 * @param {Object} Http server used by application.
 * @return {Object} instane of io socket listening to server.
 */
WebAppServer.prototype.getSocketIo = function(server) {
  var io = require('socket.io').listen(server);
  io.configure('development', function(){
    io.set('transports', ['websocket', 'xhr-polling']);
    io.set('log level', 1);
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
    params: typeof response === "string" ? JSON.parse(response) : response,
    id: request.id
  });
};

/**
 * Configures socket io handlers. Async binds socket io methods.
 * @param {Object} instance of the socket io.
 * @param {string} product of evn for socket to emit.
 * @param {string} version.
 */
WebAppServer.prototype.configureIoHandlers = function(io, product, version, cookieName, secret) {
  var self = this;
  var sockets = [];

  //Authorize and accept socket connection only if cookie exists.
  io.set('authorization', function (data, accept) {

    if (data.headers.cookie) {

      var cookies = cookie.parse(data.headers.cookie);
      var signedCookies = utils.parseSignedCookies(cookies, secret);
      var obj = utils.parseJSONCookies(signedCookies);
      data.session_id = obj[cookieName];

    } else {

      return accept('No cookie transmitted', false);

    }

    accept(null, true);
  });

  io.sockets.on('connection', function (newSocket) {

    sockets.push(newSocket);

    // Join room based on session id.
    newSocket.join(newSocket.handshake.session_id);

    newSocket.emit('env', {
      "product": product,
      "version": version,
      "credential": self.Api.credential
    });

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
            "lastStarted": 1,
            "lastStopped": 1,
            "startTime": 1,
            "endTime": 1
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
    this.logger.info("Configuration file not set ", this.config);
    return false;
  }

  var singularREST = {
    'apps': 'getApplication',
    'streams': 'getStream',
    'flows': 'getFlow',
    'mapreduce': 'getMapreduce',
    'datasets': 'getDataset',
    'procedures': 'getQuery'
  };

  var pluralREST = {
    'apps': 'getApplications',
    'streams': 'getStreams',
    'flows': 'getFlows',
    'mapreduce': 'getMapreduces',
    'datasets': 'getDatasets',
    'procedures': 'getQueries'
  };

  var typesREST = {
    'apps': 'Application',
    'streams': 'Stream',
    'flows': 'Flow',
    'mapreduce': 'Mapreduce',
    'datasets': 'Dataset',
    'procedures': 'Query'
  };

  var selectiveREST = {
    'apps': 'ByApplication',
    'streams': 'ByStream',
    'datasets': 'ByDataset'
  };

  /**
   * Enable testing.
   */
  this.app.get('/test', function (req, res) {
    res.sendfile(path.resolve(__dirname + '../../../client/test/TestRunner.html'));
  });

  /*
   * REST handler
   */
  this.app.get('/rest/*', function (req, res) {

    var path = req.url.slice(6).split('/');
    var hierarchy = {};

    self.logger.trace('GET ' + req.url);

    if (!path[path.length - 1]) {
      path = path.slice(0, path.length - 1);
    }

    var methods = [], ids = [];
    for (var i = 0; i < path.length; i ++) {
      if (i % 2) {
        ids.push(path[i]);
      } else {
        methods.push(path[i]);
      }
    }

    var method = null, params = [];

    if ((methods[0] === 'apps' || methods[0] === 'streams' ||
      methods[0] === 'datasets') && methods[1]) {

      if (ids[1]) {
        method = singularREST[methods[1]];
        params = [typesREST[methods[1]], { id: ids[1] }];
      } else {
        method = pluralREST[methods[1]] + selectiveREST[methods[0]];
        params = [ids[0]];
      }

    } else {

      if (ids[0]) {
        method = singularREST[methods[0]];
        params = [typesREST[methods[0]], { id: ids[0] } ];
      } else {
        method = pluralREST[methods[0]];
        params = [];
      }

    }

    var accountID = 'developer';

    if (method === 'getQuery' || method === 'getMapreduce') {
      params[1].application = ids[0];
    }

    if (method === 'getFlow') {

      self.Api.manager(accountID, 'getFlowDefinition', [ids[0], ids[1]],
        function (error, response) {

          if (error) {
            self.logger.error(error);
            res.status(500);
            res.send({
              error: error
            });
          } else {
            res.send(response);
          }

      });

    } else {

      self.Api.metadata(accountID, method, params, function (error, response) {

          if (error) {
            self.logger.error(error);
            res.status(500);
            res.send({
              error: error
            });
          } else {
            res.send(response);
          }

      });

    }

  });

  this.app.get('/logs/:method/:appId/:entityId/:entityType', function (req, res) {

    if (!req.params.method || !req.params.appId || !req.params.entityId || !req.params.entityType) {
      res.send('incorrect request');
    }

    var offSet = req.query.fromOffset;
    var maxSize = req.query.maxSize;
    var filter = req.query.filter;
    var method = req.params.method;
    var accountID = 'developer';
    var params = [req.params.appId, req.params.entityId, +req.params.entityType, +offSet, +maxSize, filter];

    self.logger.trace('Logs ' + method + ' ' + req.url);

    self.Api.monitor(accountID, method, params, function (error, result) {
      if (error) {
        self.logger.error(error);
      } else {
        result.map(function (item) {
          item.offset = parseInt(new Int64(new Buffer(item.offset.buffer), item.offset.offset), 10);
          return item;
        });
      }

      res.send({result: result, error: error});
    });

  });

  /*
   * RPC Handler
   */
  this.app.post('/rpc/:type/:method', function (req, res) {

    var type, method, params, accountID;

    try {

      type = req.params.type;
      method = req.params.method;
      params = req.body;
      accountID = 'developer';

    } catch (e) {

      self.logger.error(e, req.body);
      res.send({ result: null, error: 'Malformed request' });

      return;

    }

    self.logger.trace('RPC ' + type + ':' + method, params);

    switch (type) {

      case 'runnable':
        self.Api.manager(accountID, method, params, function (error, result) {
          if (error) {
            self.logger.error(error);
          }
          res.send({ result: result, error: error });
        });
        break;

      case 'fabric':
        self.Api.far(accountID, method, params, function (error, result) {
          if (error) {
            self.logger.error(error);
          }
          res.send({ result: result, error: error });
        });
        break;

      case 'gateway':
        self.Api.gateway(accountID, method, params, function (error, result) {
          if (error) {
            self.logger.error(error);
          }
          res.send({ result: result, error: error });
        });
    }

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
      host: self.config['metrics.service.host'],
      port: self.config['metrics.service.port'],
      path: '/metrics',
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Content-Length': content.length
      }
    };

    var request = http.request(options, function(response) {
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
    var accountID = 'developer';
    var sessionId = req.session.id;
    self.Api.upload(accountID, req, res, req.params.file, io.sockets["in"](sessionId));
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

    var request = http.request(options, function(response) {
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
    self.logger.warn('Port ' + self.config['node-port'] + ' is in use.');
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


