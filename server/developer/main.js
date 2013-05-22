
/**
 * Copyright (c) 2013 Continuuity, Inc.
 */

var express = require('express'),
	io = require('socket.io'),
	Int64 = require('node-int64').Int64;
	fs = require('fs'),
	xml2js = require('xml2js'),
	log4js = require('log4js'),
	http = require('http'),
	https = require('https');

var Api = require('../common/api');

/**
 * Set environment.
 */
process.env.NODE_ENV = 'development';

/**
 * Set up dev server namespace.
 */
var devServer = devServer || {};

/**
 * Dev server version.
 */
devServer.VERSION = '';

/**
 * Log level for dev server.
 */
devServer.LOG_LEVEL = 'INFO';

/**
 * App framework.
 */
devServer.app = express();

/**
 * Server.
 */
devServer.server = {};

/**
 * Socket io.
 */
devServer.io = {};

/**
 * Socket to listen to emit events and data.
 */
devServer.socket = null;

/**
 * Config.
 */
devServer.config = {};

/**
 * Configuration file pulled in and set.
 */
devServer.configSet = false;

/**
 * Sets version if a version file exists.
 */
devServer.setVersion = function() {
	try {
		devServer.VERSION = fs.readFileSync(__dirname + '../../../VERSION', 'utf8');
	} catch (e) {
		devServer.VERSION = 'UNKNOWN';
	}
};

/**
 * Configures logger.
 * @param {string} opt_appenderType log4js appender type.
 * @param {string} opt_logger log4js logger name.
 * @return {Object} instance of logger.
 */
devServer.getLogger = function(opt_appenderType, opt_loggerName) {
	var appenderType = opt_appenderType || 'console';
	var loggerName = opt_loggerName || 'Developer UI';
	log4js.configure({
		appenders: [
			{type: appenderType}
		]
	});
	var logger = log4js.getLogger(loggerName);
	logger.setLevel(devServer.LOG_LEVEL);
	return logger;
};

/**
 * Configures express server.
 */
devServer.configureExpress = function() {
	devServer.app.use(express.bodyParser());

	// Workaround to make static files work on cloud.
	if (fs.existsSync(__dirname + '/../client/')) {
		devServer.app.use(express.static(__dirname + '/../client/'));
	} else {
		devServer.app.use(express.static(__dirname + '/../../client/'));
	}
};

/**
 * Creates http server based on app framework.
 * Currently works only with express.
 * @param {Object} app framework.
 * @return {Object} instance of the http server.
 */
devServer.getServerInstance = function(app) {
  return http.createServer(app);
};

/**
 * Opens an io socket using the server.
 * @param {Object} Http server used by application.
 * @return {Object} instane of io socket listening to server.
 */
devServer.getSocketIo = function(server) {
	var io = require('socket.io').listen(server);
	io.configure('development', function(){
		io.set('transports', ['websocket', 'xhr-polling']);
		io.set('log level', 1);
	});
	return io;
};

/**
 * Defines actions in response to a recieving data from a socket.
 * @param {Object} request a socket request.
 * @param {Object} error error.
 * @param {Object} response for hte socket request.
 */
devServer.socketResponse = function(request, error, response) {
	devServer.socket.emit('exec', error, {
		method: request.method,
		params: typeof response === "string" ? JSON.parse(response) : response,
		id: request.id
	});
};

/**
 * Configures socket io handlers. Async binds socket io methods.
 * @param {Object} instance of the socket io.
 */
devServer.configureIoHandlers = function(io) {
	io.sockets.on('connection', function (newSocket) {

		devServer.socket = newSocket;
		devServer.socket.emit('env',
													{"name": "local", "version": "developer", "credential": Api.credential });

		devServer.socket.on('metadata', function (request) {
			Api.metadata('developer', request.method, request.params, function (error, response) {
				devServer.socketResponse(request, error, response);
			});
		});

		devServer.socket.on('far', function (request) {
			Api.far('developer', request.method, request.params, function (error, response) {
				devServer.socketResponse(request, error, response);
			});
		});

		devServer.socket.on('gateway', function (request) {
			Api.gateway('apikey', request.method, request.params, function (error, response) {
				devServer.socketResponse(request, error, response);
			});
		});

		devServer.socket.on('monitor', function (request) {
			Api.monitor('developer', request.method, request.params, function (error, response) {
				devServer.socketResponse(request, error, response);
			});
		});

		devServer.socket.on('manager', function (request) {
			Api.manager('developer', request.method, request.params, function (error, response) {

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
				devServer.socketResponse(request, error, response);
			});
		});
	});
};

/**
 * Binds individual expressjs routes. Any additional routes should be added here.
 */
devServer.bindRoutes = function() {

  // Check to see if config is set.
	if(!devServer.configSet) {
		devServer.logger.info("Configuration file not set ", devServer.config);
		return false;
	}
	/**
	 * Upload an Application archive.
	 */
	devServer.app.post('/upload/:file', function (req, res) {
		console.log("the socket is");
		console.log(devServer.socket);
		var accountID = 'developer';
		Api.upload(accountID, req, res, req.params.file, devServer.socket);
	});

	/**
	 * Check for new version.
	 * http://www.continuuity.com/version
	 */
	devServer.app.get('/version', function (req, res) {

		var options = {
			host: 'www.continuuity.com',
			path: '/version',
			port: '80'
		};

		res.set({
			'Content-Type': 'application-json'
		});

		http.request(options, function(response) {
			var data = '';
			response.on('data', function (chunk) {
				data += chunk;
			});

			response.on('end', function () {

				data = data.replace(/\n/g, '');

				res.send(JSON.stringify({
					current: devServer.VERSION,
					newest: data
				}));
				res.end();
			});
		}).end();

	});

	/**
	 * Get a list of push destinations.
	 */
	devServer.app.get('/destinations', function  (req, res) {

		fs.readFile(__dirname + '/.credential', 'utf-8', function (error, result) {

			res.on('error', function (e) {
				logger.trace('/destinations', e);
			});

			if (error) {

				res.write('false');
				res.end();

			} else {

				var options = {
					host: devServer.config['accounts-host'],
					path: '/api/vpc/list/' + result,
					port: devServer.config['accounts-port']
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
	devServer.app.post('/credential', function (req, res) {

		var apiKey = req.body.apiKey;

		// Write credentials to file.
		fs.writeFile(__dirname + '/.credential', apiKey,
			function (error, result) {
				if (error) {

					devServer.logger.warn('Could not write to ./.credential', error, result);
					res.write('Error: Could not write credentials file.');
					res.end();

				} else {
					Api.credential = apiKey;

					res.write('true');
					res.end();

				}
		});
	});

	/**
	 * Catch port binding errors.
	 */
	devServer.app.on('error', function () {
		devServer.logger.warn('Port ' + devServer.config['node-port'] + ' is in use.');
		process.exit(1);
	});
};

/**
 * Gets the local host.
 * @return {string} localhost ip address.
 */
devServer.getLocalHost = function() {
	var os = require('os');
	var ifaces = os.networkInterfaces();
	var localhost = '';

	for (var dev in ifaces) {
		for (var i = 0, len = ifaces[dev].length; i < len; i++) {
			var details = ifaces[dev][i];
			if (details.family == 'IPv4') {
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
 * Sets config data for application server.
 * @param {Function} opt_callback Callback function to start sever start process.
 */
devServer.getConfig = function(opt_callback) {
	fs.readFile(__dirname + '/continuuity-local.xml', function(error, result) {
		var parser = new xml2js.Parser();
		parser.parseString(result, function(err, result) {
			result = result.property;
			var localhost = devServer.getLocalHost();
			for (var item in result) {
				item = result[item];
				devServer.config[item.name] = item.value;
			}
		});
		fs.readFile(__dirname + '/.credential', "utf-8", function(error, apiKey) {
			devServer.logger.trace('Configuring with', devServer.config);
			Api.configure(devServer.config, apiKey || null);
			devServer.configSet = true;
			if (opt_callback && typeof opt_callback == "function") {
				opt_callback();
			}
		});
	});
};

/**
 * Setup io, socket and configure handlers.
 * Gets config and starts the http server.
 */
devServer.start = function() {
	devServer.getConfig(function() {
		devServer.server = devServer.getServerInstance(devServer.app);
		devServer.io = devServer.getSocketIo(devServer.server);
		devServer.configureIoHandlers(devServer.io)
		devServer.bindRoutes();
		devServer.server.listen(devServer.config['node-port']);
		devServer.logger.info('Listening on port', devServer.config['node-port']);
		devServer.logger.info(devServer.config);;
	});
};

/**
 * Entry point into node setup script.
 */
devServer.init = function() {
	devServer.logger = devServer.getLogger();
	devServer.setVersion();
	devServer.configureExpress();
};

devServer.init();
devServer.start();


/**
 * Export app.
 */
module.exports = devServer;