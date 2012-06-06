//
// Server environment configuration
//

(function () {

	this.USERNAME = 'cntnty';
	this.PASSWORD = 'realtime';

	this.configure = function (app, express, io) {
		console.log('Configuring for ' + (process.env.NODE_ENV || 'development'));

		app.get('/', function (req, res) {
			res.sendfile(__dirname + '/index.html');
		});

		if (process.env.NODE_ENV === 'production') {

			this.PORT = 80;
			this.api = require('./api_wrapper');
			this.api.configure('rest.continuuity.com', 80);
			app.use(express['static'](__dirname + '/../build/'));

		} else if (process.env.NODE_ENV === 'staging') {

			this.PORT = 80;
			this.api = require('./api_wrapper');
			this.api.configure('127.0.0.1', 8082);
			app.use(express['static'](__dirname + '/../build/'));

		} else if (process.env.NODE_ENV === 'devstaging') {

			this.PORT = 80;
			this.api = require('./api_simulator');
			this.api.configure('127.0.0.1', 8082);
			app.use(express['static'](__dirname + '/../build/'));

		} else if (process.env.NODE_ENV === 'localstaging') {

			this.PORT = 8081;
			this.api = require('./api_wrapper');
			this.api.configure('127.0.0.1', 45000);
			app.use(express['static'](__dirname + '/../client/'));

		} else {

			this.PORT = 8081;
			this.api = require('./api_simulator');
			this.api.configure('127.0.0.1', 8082);
			app.use(express['static'](__dirname + '/../client/'));
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
	};

}).call(exports);