var express = require('express'),
	app = express.createServer(express.basicAuth(function (u, p) {
		return Env.USERNAME === u && Env.PASSWORD === p;
	})),
	io = require('socket.io').listen(app);

var Env = require('./env');
Env.configure(app, express, io);

app.listen(Env.PORT);

io.sockets.on('connection', function (socket) {

	socket.on('rest', function (request) {
		console.log('Received', request);
		Env.api.request(request.method, request.params, function (response) {
			console.log('Responding', response);
			socket.emit('exec', {
				method: request.method,
				params: typeof response === "string" ? JSON.parse(response) : response,
				id: request.id
			});
		});
	});

});