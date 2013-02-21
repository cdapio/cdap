
var express = require('express'),
	io = require('socket.io'),
	Int64 = require('node-int64').Int64;
	fs = require('fs'),
	xml2js = require('xml2js'),
	log4js = require('log4js'),
	Api = require('../common/api'),
	Env = require('./env');

process.env.NODE_ENV = 'production';

/**
* Configure logger.
*/
const LOG_LEVEL = 'TRACE';
log4js.configure({
	appenders: [
		{ type : 'console' }
	]
});
var logger = process.logger = log4js.getLogger('Reactor UI');
logger.setLevel(LOG_LEVEL);

/**
 * Configure Express.
 */
app = express.createServer();
app.use(express.bodyParser());
app.use(express['static'](__dirname + '/../../client/'));

io = io.listen(app);
io.configure('production', function(){
	io.set('transports', ['websocket', 'xhr-polling']);
});

var config = {};
var sockets = {};

/**
 * SocketIO authentication
 * 
 * THIS IS INCOMPLETE.
 * 
 *
var Session = require('connect').middleware.session.Session;
io.set('authorization', function (data, accept) {

	if (data.headers.cookie) {
		data.cookie = parseCookie(data.headers.cookie);
		data.sessionID = data.cookie['express.sid'];
		// save the session store to the data object 
		// (as required by the Session constructor)
		data.sessionStore = sessionStore;
		sessionStore.get(data.sessionID, function (err, session) {
			if (err || !session) {
				accept('Error', false);
			} else {
				// create a session object, passing data as request and our
				// just acquired session data
				data.session = new Session(data, session);
				accept(null, true);
			}
		});
	} else {
		return accept('No cookie transmitted.', false);
	}
});
 */

/**
 * SocketIO handlers
 */
io.sockets.on('connection', function (socket) {

	// For reference by its sessionID elsewhere.
	socket.join(socket.handshake.sessionID);

	// Emits environment information to the client.
	socket.emit('env', {"name": "remote", "version": Env.version, "ip": Env.ip});

	function socketResponse (request, error, response) {
		socket.emit('exec', error, {
			method: request.method,
			params: typeof response === "string" ? JSON.parse(response) : response,
			id: request.id
		});
	}

	/**
	 * Metadata request. Requires an accountID.
	 */
	socket.on('metadata', function (request) {

		var accountID = 'abc123';

		Api.metadata(accountID, request.method, request.params, function (error, response) {
			socketResponse(request, error, response);
		});
	});

	/**
	 * FAR request. Requires an  accountID.
	 */
	socket.on('far', function (request) {

		var accountID = 'abc123';

		Api.far(accountID, request.method, request.params, function (error, response) {
			socketResponse(request, error, response);
		});
	});

	/**
	 * Flow Monitor request. Requires an accountID.
	 */
	socket.on('monitor', function (request) {

		var accountID = 'abc123';

		Api.monitor(accountID, request.method, request.params, function (error, response) {
			socketResponse(request, error, response);
		});
	});

	/**
	 * Flow Manager request. Requires an accountID.
	 */
	socket.on('manager', function (request) {

		var accountID = 'abc123';

		Api.manager(accountID, request.method, request.params, function (error, response) {
			
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

			socket.emit('exec', error, {
				method: request.method,
				params: typeof response === "string" ? JSON.parse(response) : response,
				id: request.id
			});
			
		});
	});

	/**
	 * Gateway request. Requires an API Key.
	 */
	socket.on('gateway', function (request) {

		var apiKey = 'abc123';

		Api.gateway(apiKey, request.method, request.params, function (error, response) {
			socketResponse(request, error, response);
		});
	});

});

/**
 * HTTP handlers.
 */
app.post('/upload/:file', function (req, res) {

	Api.upload(req, res, req.params.file, socket);

});
app.on('error', function () {
	logger.warn('Port ' + config['node-port'] + ' is in use.');
	process.exit(1);
});

/**
 * Authenticator
 */
function authenticator () {
	this.getAccountID = function (sessionID) {
		return 'developer';
	}
}

/**
 * Read configuration and start the server.
 */

//fs.readFile(__dirname + '/../developer/continuuity-site.xml',
fs.readFile(process.env.CONTINUUITY_HOME + '/conf/continuuity-site.xml',
	function (error, result) {

		if (error) {

			logger.warn('Could not find configuration file.');
			process.exit(1);

		} else {

			var parser = new xml2js.Parser();
			parser.parseString(result, function (err, result) {

				result = result.property;

				for (var item in result) {
					item = result[item];
					config[item.name] = item.value;
				}

				Env.getVersion(function (version) {
					Env.getAddress(function (error, address) {

						logger.trace('Version', version);
						logger.trace('IP Address', address);

						logger.trace('Configuring with', config);
						Api.configure(config);
					
						logger.trace('Listening on port',
							config['node-port']);	
						app.listen(config['node-port']);

					});
				});

			});

		}
});