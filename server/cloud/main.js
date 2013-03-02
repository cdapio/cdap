
/**
 * Copyright (c) 2013 Continuuity, Inc.
 */

var express = require('express'),
	http = require('http'),
	https = require('https'),
	io = require('socket.io'),
	Int64 = require('node-int64').Int64;
	fs = require('fs'),
	xml2js = require('xml2js'),
	log4js = require('log4js'),
	utils = require('connect').utils,
	cookie = require('cookie'),
	crypto = require('crypto');

var Api = require('../common/api'),
	Env = require('./env');

var LOG_LEVEL = 'INFO';
var config = {};
var sockets = {};

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
 * Read configuration and start the server.
 */
var configPath = __dirname + '/continuuity-local.xml';
if (fs.existsSync(process.env.CONTINUUITY_HOME + '/conf/continuuity-site.xml')) {
	configPath = process.env.CONTINUUITY_HOME + '/conf/continuuity-site.xml';
}

/**
 * Get user.
 */
function getUser (name, done) {

	var options = {
		hostname: config['accounts-host'],
		port: config['accounts-port'],
		path: '/api/vpc/getUser/' + name,
		method: 'GET'
	};
	https.request(options, function(result) {
		result.setEncoding('utf8');
		var data = '';
		result.on('data', function(chunk) {
			data += chunk;
		});
		result.on('end', function () {
			done(data);
		});
	}).on('error', function(e) {
		console.error(e);
	}).end();
}

/**
 * Read the configuration file.
 */

fs.readFile(configPath, function (error, result) {

	var parser = new xml2js.Parser();
	parser.parseString(result, function (err, result) {

		result = result.property;

		for (var item in result) {
			item = result[item];
			config[item.name] = item.value;
		}

		logger.info('Continuuity HOME', process.env.CONTINUUITY_HOME);
		logger.info(config);

		/**
		 * Configure Express Web server.
		 */
		var app = express();
		app.use(express.bodyParser());

		/**
		 * Express cookie sessions.
		 */
		app.use(express.cookieParser());
		app.use(express.cookieSession({
			key: 'continuuity-sso',
			secret: config['cookie-secret'],
			cookie: {
				path: '/',
				domain: process.env.NODE_ENV === 'production' ? '.continuuity.net' : '',
				maxAge: 24 * 60 * 60 * 1000
			}
		}));

		/**
		 * Session check.
		 */
		function checkSSO (req, res, next) {

			logger.trace(req.session);

			if (req.session.account_id) {

				next();

				/*

				if (req.session.account_id !== config['user']) {
					res.write('Denied (' + config['user'] + ':' + req.session.account_id + ')');
					res.end();
				} else {
					next();
				}

				*/

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
		// Root in production.
		app.get('/', checkSSO);

		// Root in development.
		app.get('/cloud/', checkSSO);

		// Redirected from central with a fresh nonce.
		// Todo: encrypt an SSO token with the user info.
		app.get('/sso/:nonce', function (req, res) {

			var nonce = req.params.nonce;

			logger.trace('< /sso/' + nonce);

			var options = {
				hostname: config['accounts-host'],
				port: config['accounts-port'],
				path: '/getSSOUser/' + nonce,
				method: 'GET'
			};

			https.request(options, function(result) {

				result.setEncoding('utf8');
				var data = '';

				result.on('data', function(chunk) {
					data += chunk;
				});

				result.on('end', function () {

					logger.trace('> /getSSOUser', data);

					if (res.statusCode !== 200) {

						res.write('Error: ' + data);
						res.end();

					} else {

						var account = JSON.parse(data);
						// Create a unique ID for this session.
						var current_date = (new Date()).valueOf().toString();
						var random = Math.random().toString();
						req.session.session_id = crypto.createHash('sha1')
							.update(current_date + random).digest('hex');

						/*
						if (account.account_id !== config['user']) {
							res.write('Denied (' + config['user'] + ':' + account.account_id + ')');
							res.end();
						} else {
						*/
							req.session.account_id = account.account_id;
							req.session.name = account.first_name + ' ' + account.last_name;
							req.session.api_key = account.api_key;
							res.redirect('/');
						/*
						}
						*/

					}

				});
			}).on('error', function(e) {
				console.error(e);
			}).end();

		});
		app.get('/logout', function (req, res) {

			req.session = null;
			res.redirect('https://accounts.continuuity.com/logout');

		});

		/**
		 * Express static directory.
		 */
		if (fs.existsSync(__dirname + '/../client/')) {
			app.use(express.static(__dirname + '/../client/'));
		} else {
			app.use(express.static(__dirname + '/../../client/'));
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
					"location": "remote",
					"version": Env.version,
					"ip": Env.ip,
					"cloud": {
						"name": config['gateway.cluster.name']
					},
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
				 * FAR request. Requires an  accountID.
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
					Api.gateway(request.session.api_key, request.method,
						request.params, function (error, response) {
						socketResponse(socket, request, error, response);
					}, true);
				});
			});

		}

		/**
		 * HTTP handlers.
		 */
		app.post('/upload/:file', function (req, res) {

			var session_id = req.session.session_id;
			Api.upload(req.session.account_id, req, res, req.params.file, io.sockets.in(session_id));

		});
		app.on('error', function () {
			logger.warn('Port ' + config['node-port'] + ' is in use.');
			process.exit(1);
		});

		getUser(config['gateway.cluster.name'], function (user) {
			config['user'] = user;
			Env.getVersion(function (version) {
				Env.getAddress(function (error, address) {

					logger.trace('Version', version);
					logger.trace('IP Address', address);
					logger.trace('User', user);

					logger.trace('Configuring with', config);
					Api.configure(config);

					/**
					 * Create an HTTP server that redirects to HTTPS.
					 */
					http.createServer(function (request, response) {

						var host = request.headers.host.split(':')[0];

						var path = 'https://' + host + ':' +
							config['cloud-ui-ssl-port'] + request.url;

						response.writeHead(302, {'Location': path});
						response.end();

					}).listen(config['cloud-ui-port']);
					logger.trace('HTTP listening on port', config['cloud-ui-port']);

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
					logger.trace('HTTPS listening on port', config['cloud-ui-ssl-port']);

					logger.info('Listening on port ');

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