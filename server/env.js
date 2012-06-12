//
// Server environment configuration
//

(function () {

	this.configure = function (app, express, io) {
		console.log('Configuring for ' + (process.env.NODE_ENV || 'development'));

		app.get('/', function (req, res) {
			res.sendfile(__dirname + '/index.html');
		});

		if (process.env.API_SIM) {
			this.api = require('./api_simulator');
		} else {
			this.api = require('./api');
		}

		app.use(express['static'](__dirname + '/../client/'));

		if (process.env.NODE_ENV === 'production') {

			this.USERNAME = 'cntnty';
			this.PASSWORD = 'realtime';

			this.PORT = 80;
			this.api.configure({
				'upload': { 'host': 'upload.continuuity.com', 'port': 45000 },
				'manager': { 'host': 'manager.continuuity.com', 'port': 45001 },
				'monitor': { 'host': 'monitor.continuuity.com', 'port': 45002 }
			});

		} else if (process.env.NODE_ENV === 'staging') {

			this.USERNAME = 'cntnty';
			this.PASSWORD = 'realtime';

			this.PORT = 80;
			this.api.configure({
				'upload': { 'host': '127.0.0.1', 'port': 45000 },
				'manager': { 'host': '127.0.0.1', 'port': 45001 },
				'monitor': { 'host': '127.0.0.1', 'port': 45002 }
			});

		} else {

			this.PORT = 9999;
			this.api.configure({
				'upload': { 'host': '127.0.0.1', 'port': 45000 },
				'manager': { 'host': '127.0.0.1', 'port': 45001 },
				'monitor': { 'host': '127.0.0.1', 'port': 45002 }
			});

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