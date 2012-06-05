
var http = require('http'),
	thrift = require('thrift'),
	fs = require('fs');

var ManagerService = require('./gen-nodejs/ManagerService.js'),
	MonitorService = require('./gen-nodejs/MonitorService.js'),
	SubscriptionService = require('./gen-nodejs/SubscriptionService.js'),
	engine_types = require('./gen-nodejs/engine_types.js');

(function () {

	this.configure = function (host, port) {

		this.connection = thrift.createConnection(host, port);
		this.connection.on('error', function (error) {
			console.log('THRIFT ERROR', error);
		});

		this.Manager = thrift.createClient(ManagerService, this.connection);
		this.Monitor = thrift.createClient(MonitorService, this.connection);

		Subscriber.start();

	};

	this.request = function (method, params, done) {
		
		if (method in this.Manager) {
			done('Unknown method for service Manager: ' + method, null);
		} else {
			this.Manager[method].apply(this.Manager, params.concat(done));
		}

	};

	this.status = function (params, done) {

		this.Monitor.getFlowStatus(params.concat(done));

	};

	this.upload = function (req, res) {
		var body = '';
		var header = '';

		var content_type = req.headers['content-type'];
		var boundary = '';//content_type.split('; ')[1].split('=')[1];
		var content_length = parseInt(req.headers['content-length'], 10);
		var headerFlag = true;
		var filename = 'dummy.bin';
		var filenameRegexp = /filename="(.*)"/m;
		console.log('content-type: ' + content_type);
		console.log('boundary: ' + boundary);
		console.log('content-length: ' + content_length);

		req.on('data', function(raw) {
			console.log('received data length: ' + raw.length);
			var i = 0;
			while (i < raw.length) {
				if (headerFlag) {
					var chars = raw.slice(i, i+4).toString();
					if (chars === '\r\n\r\n') {
						headerFlag = false;
						header = raw.slice(0, i+4).toString();
						console.log('header length: ' + header.length);
						console.log('header: ');
						console.log(header);
						i = i + 4;
						// get the filename
						var result = filenameRegexp.exec(header);
						if (result[1]) {
							filename = result[1];
						}
						console.log('filename: ' + filename);
						console.log('header done');
					}
					else {
						i += 1;
					}
				}
				else {
					// parsing body including footer
					body += raw.toString('binary', i, raw.length);
					i = raw.length;
					console.log('actual file size: ' + body.length);
				}
			}
		});

		req.on('end', function() {
			// removing footer '\r\n'--boundary--\r\n' = (boundary.length + 8)
			body = body.slice(0, body.length - (boundary.length + 8));
			console.log(body);
			console.log('final file size: ' + body.length);
			fs.writeFileSync('files/' + filename, body, 'binary');
			console.log('done');
			res.redirect('back');
		});
	};

	var Subscriber = {
		server: thrift.createServer(SubscriptionService, {
			fire: function (id, event) {
				this.map[id](event);
				return true;
			},
			ping: function () {
				return 'Pong';
			}
		}),
		start: function () {
			this.server.listen(1000);
		},
		register: function (id, callback) {
			this.map[id] = callback;
		},
		unregister: function (id) {
			delete this.map[id];
		}
	};

	this.subscribe = function (ns, name, endpoint, callback) {

		this.Monitor.subscribe(ns, name, endpoint, function (error, response) {

			if (error !== null) {
				console.log('SUB ERROR', ns, name, endpoint, error);
			} else {
				Subscriber.register(response.id, callback);
			}

		});

	};

	this.unsubscribe = function (ns, name) {

		this.Monitor.unsubscribe(ns, name, function (error, response) {

			if (error !== null) {
				console.log('UNSUB ERROR', ns, name, error);
			} else {
				Subscriber.unregister(response.id);
			}

		});

	};

}).call(exports);