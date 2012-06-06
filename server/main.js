var Env, express = require('express'),
	app = express.createServer(express.basicAuth(function (u, p) {
		return Env.USERNAME === u && Env.PASSWORD === p;
	})),
	io = require('socket.io').listen(app);

app.use(express.bodyParser());

Env = require('./env');
Env.configure(app, express, io);

app.listen(Env.PORT);

var id = "default";
var sockets = {};

app.post('/upload', function (req, res) {

	Env.api.upload(req, res, sockets[id]);

});

io.sockets.on('connection', function (socket) {

	sockets[id] = socket;

	socket.on('rest', function (request) {
		console.log('Received', request);
		Env.api.request(request.method, request.params, function (error, response) {
			console.log('Responding', response);
			socket.emit('exec', {
				method: request.method,
				params: typeof response === "string" ? JSON.parse(response) : response,
				id: request.id
			});
		});
	});

});