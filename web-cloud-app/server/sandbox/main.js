
/**
 * Copyright (c) 2013 Continuuity, Inc.
 */

var express = require('express'),
	http = require('http'),
	https = require('https'),
	io = require('socket.io'),
	Int64 = require('node-int64').Int64,
	fs = require('fs'),
	xml2js = require('xml2js'),
	log4js = require('log4js'),
	utils = require('connect').utils,
	cookie = require('cookie'),
	crypto = require('crypto'),
	diskspace = require('diskspace');

var Api = require('../common/api'),
	Env = require('./env');

var LOG_LEVEL = 'INFO';
var config = {};
var sockets = {};

/**
 * By default.
 */
process.env.NODE_ENV = 'development';

/**
* Configure logger.
*/
log4js.configure({
	appenders: [
		{ type : 'console' }
	]
});
var logger = process.logger = log4js.getLogger('Cloud UI');
logger.setLevel(LOG_LEVEL);

/**
 * Catch anything uncaught.
 */
process.on('uncaughtException', function (err) {
	logger.error('Uncaught Exception', err);
});

/**
 * Read configuration and start the server.
 */
var configPath = __dirname + '/continuuity-local.xml';
if (fs.existsSync(process.env.CONTINUUITY_HOME + '/conf/continuuity-site.xml')) {
	configPath = process.env.CONTINUUITY_HOME + '/conf/continuuity-site.xml';

	// If the production config exists, we're in production.
	process.env.NODE_ENV = 'production';

}

/**
 * Make a request to the Accounts system.
 */
function accountsRequest (path, done) {

	logger.info('Requesting from accounts, ', path);

	var options = {
		hostname: config['accounts-host'],
		port: config['accounts-port'],
		path: path,
		method: 'GET'
	};

	var lib;

	if (process.env.NODE_ENV === 'production' || +config['accounts-port'] === 443) {
		lib = https;
	} else {
		lib = http;
	}

	var req = lib.request(options, function(result) {
		result.setEncoding('utf8');
		var data = '';
		result.on('data', function(chunk) {
			data += chunk;
		});
		result.on('end', function () {

			logger.info('Response from accounts', result.statusCode, data);

			var status;

			try {
				data = JSON.parse(data);
				status = result.statusCode;
			} catch (e) {
				logger.warn('Parsing error', e, data);
				data = e;
				status = 500;
			}

			done(status, data);

		});
	}).on('error', function(e) {
		logger.warn(e);
		done(500, e);
	}).end();

}

function getRoot () {
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
}

function renderError(req, res) {

	try {
		res.sendfile('internal-error.html', {'root': getRoot()});
	} catch (e) {
		res.write('Internal error. Please email <a href="mailto:support@continuuity.com">support@continuuity.com</a>.');
	}

}

function renderAccessError(req, res) {

	if (req.session && config.info && config.info.owner) {
		logger.warn('Denied user (current, owner)', req.session.account_id, config['info'].owner.account_id);
	}

	try {
		res.sendfile('access-error.html', {'root': getRoot()});
	} catch (e) {
		res.write('Access error. <a href="https://accounts.continuuity.com/">Account Home</a>.');
	}

}

/**
 * Read the configuration file.
 */

fs.readFile(configPath, function (error, result) {

	var parser = new xml2js.Parser();
	parser.parseString(result, function (err, result) {

		/**
		 * Check configuration file error.
		 */
		if (err) {
			logger.error('Error reading config! Aborting!');
			logger.error(err, result);
			process.exit(1);
			return;
		}

		result = result.property;

		for (var item in result) {
			item = result[item];
			config[item.name] = item.value;
		}

		/**
		 * Display configuration.
		 */
		logger.info('Configuring with', config);
		logger.info('CONTINUUITY_HOME is', process.env.CONTINUUITY_HOME);
		logger.info('NODE_ENV is', process.env.NODE_ENV);

		/**
		 * Check cluster name.
		 */
		if (typeof config['gateway.cluster.name'] !== 'string') {
			logger.error('No cluster name in config! Aborting!');
			process.exit(1);
			return;
		}

		/**
		 * Configure Express Web server.
		 */
		var app = express();
		app.use(express.bodyParser());

		/**
		 * Log HTTP requests
		 */
		app.use(function (req, res, next) {

			logger.trace(req.method + ' ' + req.url + (req.account ? '(' + req.account.account_id + ')' : ''));
			next();

		});

		/**
		 * Express cookie sessions.
		 */
		app.use(express.cookieParser());
		app.use(express.cookieSession({
			key: 'continuuity-sso',
			secret: config['cookie-secret'],
			cookie: {
				path: '/',
				domain: '.continuuity.net',
				maxAge: 24 * 60 * 60 * 1000
			}
		}));

		/**
		 * Session check.
		 */
		function checkSSO (req, res, next) {

			if (req.session.account_id) {

				if (!config['info'].owner || !config['info'].owner.account_id) {

					logger.error('Checking SSO. Owner information not found in the configuration!');
					renderError(req, res);

				} else {
					if (req.session.account_id !== config['info'].owner.account_id) {
						renderAccessError(req, res);
					} else {
						next();
					}
				}

			} else {

				var ret = config['gateway.cluster.name'];
				var host = config['accounts-host'];
				if (config['accounts-port'] !== '443') {
					host += ':' + config['accounts-port'];
				}

				res.redirect('https://' + host +
					'/sso?return=' + encodeURIComponent(ret));

			}

		}

		// Check SSO.
		app.get('/', checkSSO);

		// Redirected from central with a fresh nonce.
		// Todo: encrypt an SSO token with the user info.
		app.get('/sso/:nonce', function (req, res) {

			var nonce = req.params.nonce;

			logger.info('SSO Inbound for nonce', nonce);

			accountsRequest('/getSSOUser/' + nonce, function (status, account) {

				logger.info(arguments);

				if (status !== 200 || account.error) {

					logger.warn('getSSOUser', status, account);
					logger.warn('SSO Failed. Redirecting to https://' + config['accounts-host']);
					res.redirect('https://' + config['accounts-host']);
					res.end();

				} else {

					// Create a unique ID for this session.
					var current_date = (new Date()).valueOf().toString();
					var random = Math.random().toString();
					req.session.session_id = crypto.createHash('sha1')
						.update(current_date + random).digest('hex');

					// Perform ownership check.
					if (process.env.NODE_ENV === 'production') {

						if (!config['info'].owner || !config['info'].owner.account_id) {

							logger.error('Inbound SSO. Owner information not found in the configuration!');
							renderError(req, res);

						} else {

							if (account.account_id !== config['info'].owner.account_id) {
								renderAccessError(req, res);
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

		/**
		 * Express static directory.
		 */
		if (fs.existsSync(__dirname + '/../client/')) {
			app.use(express["static"](__dirname + '/../client/'));
		} else {
			app.use(express["static"](__dirname + '/../../client/'));
		}

		/**
		 * SocketIO handlers
		 */
		function socketResponse (socket, request, error, response) {
			socket.emit('exec', error, {
				method: request.method,
				params: typeof response === "string" ? JSON.parse(response) : response,
				id: request.id
			});
		}
		function setSocketHandlers () {

			/**
			 * SocketIO cookie sessions
			 */
			io.set('authorization', function (data, accept) {

				if (data.headers.cookie) {

					var cookies = cookie.parse(data.headers.cookie);
					var signedCookies = utils.parseSignedCookies(cookies, config['cookie-secret']);
					var obj = utils.parseJSONCookies(signedCookies);

					data.api_key = obj['continuuity-sso'].api_key;
					data.account_id = obj['continuuity-sso'].account_id;
					data.name = obj['continuuity-sso'].name;

					if (data.account_id) {

						/**
						 * Used to make Sockets available to HTTP requests.
						 */
						data.session_id = obj['continuuity-sso'].account_id;

						return accept(null, true);

					} else {
						return accept('No session detected.', false);
					}

				} else {

					return accept('No cookie transmitted.', false);

				}
			});

			/**
			 * SocketIO per-socket handlers
			 */
			io.sockets.on('connection', function (socket) {

				/**
				 * For reference by request.session.account_id elsewhere.
				 */
				socket.join(socket.handshake.session_id);

				/**
				 * Emits environment information to the client.
				 */

				socket.emit('env', {
					"product": "Sandbox",
					"location": "remote",
					"version": Env.version || 'UNKNOWN',
					"ip": Env.ip,
					"cluster": config['info'],
					"account": {
						"account_id": socket.handshake.account_id,
						"name": socket.handshake.name
					}
				});

				/**
				 * Metadata request. Requires an accountID.
				 */
				socket.on('metadata', function (request) {

					Api.metadata(socket.handshake.account_id,
						request.method, request.params, function (error, response) {
						socketResponse(socket, request, error, response);
					});

				});

				/**
				 * FAR request. Requires an	accountID.
				 */
				socket.on('far', function (request) {

					Api.far(socket.handshake.account_id,
						request.method, request.params, function (error, response) {
						socketResponse(socket, request, error, response);
					});
				});

				/**
				 * Flow Monitor request. Requires an accountID.
				 */
				socket.on('monitor', function (request) {

					Api.monitor(socket.handshake.account_id,
						request.method, request.params, function (error, response) {
						socketResponse(socket, request, error, response);
					});
				});

				/**
				 * Flow Manager request. Requires an accountID.
				 */
				socket.on('manager', function (request) {

					Api.manager(socket.handshake.account_id,
						request.method, request.params, function (error, response) {

						if (response && response.length) {
							var int64values = {
								"lastStarted": 1,
								"lastStopped": 1,
								"startTime": 1,
								"endTime": 1
							};

							// Hax. Int64 is not being jsonized nicely.
							for (var i = 0; i < response.length; i ++) {
								for (var j in response[i]) {
									if (j in int64values) {
										response[i][j] = parseInt(response[i][j].toString(), 10);
									}
								}
							}
						}

						socketResponse(socket, request, error, response);

					});
				});

				/**
				 * Gateway request. Requires an API Key.
				 */
				socket.on('gateway', function (request) {
					Api.gateway(socket.handshake.api_key, request.method,
						request.params, function (error, response) {
						socketResponse(socket, request, error, response);
					}, true);
				});
			});

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

		/*
		 * REST handler
		 */
		app.get('/rest/*', function (req, res) {

			var path = req.url.slice(6).split('/');
			var hierarchy = {};

			logger.trace('GET ' + req.url);

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

			var accountID = req.session.account_id;

			if (method === 'getQuery' || method === 'getMapreduce') {
				params[1].application = ids[0];
			}

			if (method === 'getFlow') {

				Api.manager(accountID, 'getFlowDefinition', [ids[0], ids[1]],
					function (error, response) {

						if (error) {
							logger.error(error);
							res.status(500);
							res.send({
								error: error
							});
						} else {
							res.send(response);
						}

				});

			} else {

				Api.metadata(accountID, method, params, function (error, response) {

						if (error) {
							logger.error(error);
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

		app.get('/logs/:method/:appId/:entityId/:entityType', function (req, res) {

			if (!req.params.method || !req.params.appId || !req.params.entityId || !req.params.entityType) {
				res.send('incorrect request');
			}

			var offSet = req.query.fromOffset;
			var maxSize = req.query.maxSize;
			var filter = req.query.filter;
			var method = req.params.method;
			var accountID = req.session.account_id;

			var params = [req.params.appId, req.params.entityId, +req.params.entityType, +offSet, +maxSize, filter];

			logger.trace('Logs ' + method + ' ' + req.url);

			Api.monitor(accountID, method, params, function (error, result) {
				if (error) {
					logger.error(error);
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
		app.post('/rpc/:type/:method', function (req, res) {

			var type, method, params, accountID;

			try {

				type = req.params.type;
				method = req.params.method;
				params = req.body;
				accountID = req.session.account_id;

			} catch (e) {

				logger.error(e, req.body);
				res.send({ result: null, error: 'Malformed request' });

				return;

			}

			logger.trace('RPC ' + type + ':' + method, params);

			switch (type) {

				case 'runnable':
					Api.manager(accountID, method, params, function (error, result) {
						if (error) {
							logger.error(error);
						}
						res.send({ result: result, error: error });
					});
					break;

				case 'fabric':
					Api.far(accountID, method, params, function (error, result) {
						if (error) {
							logger.error(error);
						}
						res.send({ result: result, error: error });
					});
					break;

				case 'gateway':

					// NOTE: Gateway authenticates with the API key, not the AccountID.
					Api.gateway(req.session.api_key, method, params, function (error, result) {
						if (error) {
							logger.error(error);
						}
						res.send({ result: result, error: error });
					});
			}

		});

		/*
		 * Metrics Handler
		 */
		app.post('/metrics', function (req, res) {

			var pathList = req.body;
			var accountID = req.session.account_id;

			logger.trace('Metrics ', pathList);

			var content = JSON.stringify(pathList);

			var options = {
				host: config['metrics.service.host'],
				port: config['metrics.service.port'],
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
						logger.error('Parsing Error', data);
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
		 * HTTP handlers.
		 */
		app.post('/upload/:file', function (req, res) {

			var session_id = req.session.session_id;
			Api.upload(req.session.account_id, req, res, req.params.file, io.sockets["in"](session_id));

		});

		/**
		 * Disk space.
		 */
		app.get('/disk', function (req, res) {

			var path = process.env.NODE_ENV === 'production' ? '/dev/vdb1' : '/';

			diskspace.check(path, function (total, free, status) {

				res.write(JSON.stringify({
					total: total,
					free: free,
					status: status
				}));
				res.end();

			});

		});

		/**
		 * Bind error
		 */
		app.on('error', function () {

			logger.warn('Port ' + config['node-port'] + ' is in use.');
			process.exit(1);

		});

		accountsRequest('/api/vpc/getUser/' + config['gateway.cluster.name'],
			function (status, info) {

				if (status !== 200 || !info) {
					logger.error('No cluster info received from Accounts! Aborting!');
					process.exit(1);
					return;
				}

				config['info'] = info;
				Env.getVersion(function (version) {
					Env.getAddress(function (error, address) {

						logger.info('Version', version);
						logger.info('IP Address', address);

						Api.configure(config);

						/**
						 * Create an HTTP server that redirects to HTTPS.
						 */
						http.createServer(function (request, response) {

							var host;
							if (request.headers.host) {
								host = request.headers.host.split(':')[0];
							} else {
								host = config['gateway.cluster.name'] + '.continuuity.net';
							}

							var path = 'https://' + host + ':' +
								config['cloud-ui-ssl-port'] + request.url;

							logger.trace('Redirected HTTP to HTTPS');

							response.writeHead(302, {'Location': path});
							response.end();

						}).listen(config['cloud-ui-port']);

						/*
						 * Don't change this.
						 * Reactor start-up script looks for output "Listening on port "
						 */
						logger.info('Listening on port (HTTP)', config['cloud-ui-port']);

						/**
						 * HTTPS credentials
						 */
						var certs = {
							ca: fs.readFileSync(__dirname + '/certs/STAR_continuuity_net.ca-bundle'),
							key: fs.readFileSync(__dirname + '/certs/continuuity-com-key.key'),
							cert: fs.readFileSync(__dirname + '/certs/STAR_continuuity_net.crt')
						};

						/**
						 * Create the HTTPS server
						 */
						var server = https.createServer(certs, app).listen(config['cloud-ui-ssl-port']);

						/*
						 * Don't change this.
						 * Reactor start-up script looks for output "Listening on port "
						 */
						logger.info('Listening on port (HTTPS)', config['cloud-ui-ssl-port']);

						/**
						 * Configure Socket IO
						 */
						io = io.listen(server, certs);
						io.configure('production', function(){
							io.set('transports', ['websocket', 'xhr-polling']);
							io.enable('browser client minification');
							io.enable('browser client gzip');
							io.set('log level', 1);
							io.set('resource', '/socket.io');
						});

						/**
						 * Set the handlers after io has been configured.
						 */
						setSocketHandlers();

					});
				});
		});
	});
});