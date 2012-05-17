var express = require('express'),
	app = express.createServer(),
	io = require('socket.io').listen(app),
	http = require('http');

app.get('/', function (req, res) {
	res.sendfile(__dirname + '/index.html');
});

var Env = {};

if (process.env.NODE_ENV === 'production') {
	app.use(express['static'](__dirname + '/../build/'));
	Env.REST_HOSTNAME = 'rest.continuuity.com',
	Env.REST_PORT = 80,
	Env.MONITOR_HOSTNAME = 'monitor.continuuity.com',
	Env.MONITOR_PORT = 80;
} else {
	app.use(express['static'](__dirname + '/../client/'));
	Env.REST_HOSTNAME = '127.0.0.1',
	Env.REST_PORT = 8082,
	Env.MONITOR_HOSTNAME = '127.0.0.1',
	Env.MONITOR_PORT = 8083;
}

io.configure('production', function(){
	io.enable('browser client minification');
	io.enable('browser client etag');
	io.enable('browser client gzip');
	io.set('log level', 1);
	io.set('transports', [
		'websocket',
		'flashsocket',
		'htmlfile',
		'xhr-polling',
		'jsonp-polling'
		]);
});

io.configure('development', function(){
	io.set('transports', ['websocket']);
});

function make_request (host, port, uri, done) {
	var client = http.createClient(port, host);
	var request = client.request('GET', uri);
	request.on('response', function (response) {
		var data = '';
		response.setEncoding('utf8');
		response.on('data', function (chunk) {
			// TODO: Append
			data += chunk;
		});
		response.on('end', function () {
			done(data);
		});
	});
	request.end();
}

var listeners = [];
var services = {
	'rest': {
		request: function (method, params, done) {
			make_request(Env.REST_HOSTNAME, Env.REST_PORT, '/' + method + '/' + (params || ''), done);
		}
	},
	'monitor': {
		request: function (method, params, done) {
			make_request(Env.MONITOR_HOSTNAME, Env.MONITOR_PORT, '/' + method + '/' + (params || ''), done);
		}
	}
};

io.sockets.on('connection', function (socket) {
	listeners.push(socket);

	socket.on('rest', function (request) {
		console.log('Received', request);
		services.rest.request(request.method, request.params, function (response) {
			console.log('Responding', response);
			socket.emit('exec', {
				method: request.method,
				params: JSON.parse(response),
				id: request.id
			});			
		});
	});
});

app.listen(8081);