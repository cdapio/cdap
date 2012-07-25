
var Env = require('./env'), express = require('express'), app;

if (Env.USERNAME) {
	app = express.createServer(express.basicAuth(function (u, p) {
		return Env.USERNAME === u && Env.PASSWORD === p;
	}));
} else {
	app = express.createServer();
}
var io = require('socket.io').listen(app);
app.use(express.bodyParser());

Env.configure(app, express, io);

var id = "default";
var sockets = {};

app.post('/upload/:file', function (req, res) {

	Env.api.upload(req, res, req.params.file, sockets[id]);

});

io.sockets.on('connection', function (socket) {

	sockets[id] = socket;

	socket.emit('env', process.env.NODE_ENV || 'development');

	socket.on('manager', function (request) {
		console.log('Manager Request', request);
		Env.api.manager(request.method, request.params, function (error, response) {
			
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
			
			socket.emit('exec', error, response);

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

app.listen(Env.PORT);
