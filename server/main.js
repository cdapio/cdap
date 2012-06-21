
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
app.listen(Env.PORT);

var id = "default";
var sockets = {};

app.post('/upload', function (req, res) {

	Env.api.upload(req, res, sockets[id]);

});

io.sockets.on('connection', function (socket) {

	sockets[id] = socket;

	socket.on('manager', function (request) {
		console.log('Manager Request', request);
		Env.api.manager(request.method, request.params, function (error, response) {
			if (error) {
				socket.emit('failure', error);
			} else {
				socket.emit('exec', {
					method: request.method,
					params: typeof response === "string" ? JSON.parse(response) : response,
					id: request.id
				});
			}
		});
	});

	socket.on('gateway', function (request) {
		console.log('Gateway Request');
		Env.api.gateway(request.method, request.params, function (error, response) {
			if (error) {
				socket.emit('failure', error);
			}
		});
	});

	socket.on('monitor', function (request) {
		console.log('Monitor Request', request);
		Env.api.monitor(request.method, request.params, function (error, response) {
			if (error) {
				socket.emit('failure', error);
			} else {
				socket.emit('exec', {
					method: request.method,
					params: typeof response === "string" ? JSON.parse(response) : response,
					id: request.id
				});
			}
		});
	});

});