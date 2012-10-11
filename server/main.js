
var Env = require('./env'), express = require('express'), app;

var Int64 = require('node-int64').Int64;

process.env.NODE_ENV = process.env.NODE_ENV || 'development';

if (process.env.NODE_ENV !== 'development') {
	app = express.createServer(express.basicAuth(function (u, p) {
		if (Env.USERNAME) {
			return Env.USERNAME === u && Env.PASSWORD === p;
		} else {
			return true;
		}
	}));
} else {
	app = express.createServer();
}
var io = require('socket.io').listen(app);
app.use(express.bodyParser());

var id = "default";
var sockets = {};

app.post('/upload/:file', function (req, res) {

	Env.api.upload(req, res, req.params.file, sockets[id]);

});

io.sockets.on('connection', function (socket) {

	sockets[id] = socket;

	socket.emit('env', process.env.NODE_ENV || 'development');

	socket.on('metadata', function (request) {
		console.log('MetaData Request', request);
		Env.api.metadata(request.method, request.params, function (error, response) {
			socket.emit('exec', error, {
				method: request.method,
				params: typeof response === "string" ? JSON.parse(response) : response,
				id: request.id
			});
		});
	});

	socket.on('manager', function (request) {
		console.log('Manager Request', request);
		Env.api.manager(request.method, request.params, function (error, response) {
			
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

	socket.on('far', function (request) {
		console.log('FAR Request', request);
		Env.api.far(request.method, request.params, function (error, response) {
			
			socket.emit('exec', error, {
				method: request.method,
				params: typeof response === "string" ? JSON.parse(response) : response,
				id: request.id
			});
			
		});
	});

	socket.on('gateway', function (request) {
		console.log('Gateway Request');
		Env.api.gateway(request.method, request.params, function (error, response) {
			
			socket.emit('exec', error, {
				method: request.method,
				params: typeof response === "string" ? JSON.parse(response) : response,
				id: request.id
			});

		});
	});

	socket.on('monitor', function (request) {
		console.log('Monitor Request', request);
		Env.api.monitor(request.method, request.params, function (error, response) {


			socket.emit('exec', error, {
				method: request.method,
				params: typeof response === "string" ? JSON.parse(response) : response,
				id: request.id
			});
		
		});
	});

});

app.on('error', function () {
	console.log('Error: port ' + Env.PORT + ' is in use.');
	process.exit(1);
});

Env.configure(app, express, io, function (success) {

	if (success) {
		app.listen(Env.PORT);
		console.log('Listening on port ' + Env.PORT + '. Services:');
		console.log(Env.api.config);
	} else {
		process.exit(1);
	}
});