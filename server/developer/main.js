
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
 * Setting the environment to Cloud.
 */
process.env.NODE_ENV = 'development';

var VERSION;
try {
	VERSION = fs.readFileSync(__dirname + '../../../VERSION', 'utf8');
} catch (e) {
	VERSION = 'UNKNOWN';
}

/**
 * Configure logger.
 */
var LOG_LEVEL = 'INFO';
log4js.configure({
	appenders: [
		{ type : 'console' }
	]
});
var logger = process.logger = log4js.getLogger('Developer UI');
logger.setLevel(LOG_LEVEL);

/**
 * Configure Express.
 */
var app = express();
app.use(express.bodyParser());
if (fs.existsSync(__dirname + '/../client/')) {
	app.use(express.static(__dirname + '/../client/'));
} else {
	app.use(express.static(__dirname + '/../../client/'));
}

var server = http.createServer(app);

io = require('socket.io').listen(server);
io.configure('development', function(){
	io.set('transports', ['websocket', 'xhr-polling']);
	//io.set('log level', 1);
});

var config = {};
var socket = null;

/**
 * SocketIO handlers
 */
io.sockets.on('connection', function (newSocket) {

	socket = newSocket;
	socket.emit('env', {"name": "local", "version": "developer", "credential": Api.credential });

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
		Api.gateway('apikey', request.method, request.params, function (error, response) {
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
 * Upload an Application archive.
 */
app.post('/upload/:file', function (req, res) {

	var accountID = 'developer';
	Api.upload(accountID, req, res, req.params.file, socket);				
	
});

/**
 * Check for new version.
 * http://www.continuuity.com/version
 */
app.get('/version', function (req, res) {

	var options = {
		host: 'www.continuuity.com',
		path: '/version',
		port: '80'
	};

	http.request(options, function(response) {
		var data = '';
		response.on('data', function (chunk) {
			data += chunk;
		});

		response.on('end', function () {
			
			data = data.replace(/\n/g, '');

			res.send(JSON.stringify({
				current: VERSION,
				newest: data
			}));
			res.end();

		});
	}).end();

});

/**
 * Get a list of push destinations.
 */
app.get('/destinations', function  (req, res) {

	fs.readFile(__dirname + '/.credential', 'utf-8', function (error, result) {

		if (error) {

			res.write('false');
			res.end();

		} else {

			var options = {
				host: config['accounts-host'],
				path: '/api/vpc/list/' + result,
				port: config['accounts-port']
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
app.post('/credential', function (req, res) {

	var apiKey = req.body.apiKey;

	logger.info('Writing API Key to file', __dirname + '/.credential', apiKey);

	// Write down credentials.
	fs.writeFile(__dirname + '/.credential', apiKey,
		function (error, result) {

		if (error) {

			logger.warn('Could not write to ./.credential', error, result);
			res.write('Error: Could not write credentials file.');
			res.end();

		} else {

			res.write('true');
			res.end();

		}

	});

});

/**
 * Catch port binding errors.
 */
app.on('error', function () {
	logger.warn('Port ' + config['node-port'] + ' is in use.');
	process.exit(1);
});

function getLocalHost () {

	var os = require('os');
	var ifaces = os.networkInterfaces();
	var localhost = '';

	for (var dev in ifaces) {
		var alias=0;
		ifaces[dev].forEach(function(details){
			if (details.family=='IPv4') {
				++alias;
				if (dev === 'lo0') {
					localhost = details.address;
				}
			}
		});
	}

	return localhost;
	
}

/**
 * Read configuration and start the server.
 */
fs.readFile(__dirname + '/continuuity-local.xml',
	function (error, result) {

	var parser = new xml2js.Parser();
	parser.parseString(result, function (err, result) {

		result = result.property;
		var localhost = getLocalHost();

		for (var item in result) {
			item = result[item];
			
			config[item.name] = item.value;
		}

		/**
		 * Pull in stored credentials.
		 */
		fs.readFile(__dirname + '/.credential', "utf-8", function (error, apiKey) {

			logger.trace('Configuring with', config);
			Api.configure(config, apiKey || null);

			logger.info('Listening on port',
				config['node-port']);	
			server.listen(config['node-port']);

			logger.info(config);

		});

	});

});

