
var express = require('express'),
	io = require('socket.io'),
	Int64 = require('node-int64').Int64;
	fs = require('fs'),
	xml2js = require('xml2js'),
	log4js = require('log4js'),
	Api = require('../common/api');

process.env.NODE_ENV = 'development';

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
io.configure('development', function(){
	io.set('transports', ['websocket', 'xhr-polling']);
});

var config = {};
var socket = null;

/**
 * SocketIO handlers
 */
io.sockets.on('connection', function (newSocket) {

	socket = newSocket;
	socket.emit('env', {"name": "local", "version": "developer"});

	function socketResponse (request, error, response) {
		socket.emit('exec', error, {
			method: request.method,
			params: typeof response === "string" ? JSON.parse(response) : response,
			id: request.id
		});
	}

	socket.on('metadata', function (request) {
		Api.metadata('developer', request.method, request.params, function (error, response) {
			socketResponse(request, error, response);
		});
	});

	socket.on('far', function (request) {
		Api.far('developer', request.method, request.params, function (error, response) {
			socketResponse(request, error, response);
		});
	});

	socket.on('gateway', function (request) {
		Api.gateway('developer', request.method, request.params, function (error, response) {
			socketResponse(request, error, response);
		});
	});

	socket.on('monitor', function (request) {
		Api.monitor('developer', request.method, request.params, function (error, response) {
			socketResponse(request, error, response);
		});
	});

	socket.on('manager', function (request) {
		Api.manager('developer', request.method, request.params, function (error, response) {
			
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
	this.getAccountID = function () {
		return 'developer';
	}
}

/**
 * Read configuration and start the server.
 */
fs.readFile(__dirname + '/continuuity-site.xml',
	function (error, result) {

	var parser = new xml2js.Parser();
	parser.parseString(result, function (err, result) {

		result = result.property;

		for (var item in result) {
			item = result[item];
			config[item.name] = item.value;
		}

		logger.trace('Configuring with', config);
		Api.configure(config, new authenticator());
	
		logger.trace('Listening on port',
			config['node-port']);	
		app.listen(config['node-port']);

	});

});

