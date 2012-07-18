
var http = require('http'),
	thrift = require('thrift'),
	fs = require('fs');

var FARService = require('./thrift_bindings/FARService.js'),
	FlowService = require('./thrift_bindings/FlowService.js'),
	flowservices_types = require('./thrift_bindings/flowservices_types.js');
var FlowMonitor = require('./thrift_bindings/FlowMonitor.js'),
	flowmonitor_types = require('./thrift_bindings/flowmonitor_types.js');
var SubscriptionService;
var ttransport, tprotocol;

try {
	ttransport = require('thrift/lib/thrift/transport');
} catch (e) {
	ttransport = require('thrift/transport');
}
try {
	tprotocol = require('thrift/lib/thrift/protocol');
} catch (e) {
	tprotocol = require('thrift/protocol');
}

(function () {

	this.config = {};
	this.configure = function (config) {
		this.config = config;
	};

	this.manager = function (method, params, done) {

		params = params || [];

		var conn = thrift.createConnection(
			this.config.manager.host,
			this.config.manager.port, {
			transport: ttransport.TFramedTransport,
			protocol: tprotocol.TBinaryProtocol
		});

		conn.on('error', function (error) {
			console.log('FlowManager: ', error);
			done('Could not connect to FlowMonitor.');
		});
		
		var Manager = thrift.createClient(FlowService, conn);

		console.log(method, params);

		var identifier;

		switch (method) {
			case 'start':
				identifier = new flowservices_types.FlowDescriptor({
					identifier: new flowservices_types.FlowIdentifier({
						app: params[0],
						flow: params[1],
						version: parseInt(params[2], 10),
						accountId: 'demo'
					}),
					"arguments": []
				});
				Manager.start(null, identifier, done);
				conn.end();
				
			break;
			case 'stop':
				identifier = new flowservices_types.FlowIdentifier({
					app: params[0],
					flow: params[1],
					version: parseInt(params[2], 10),
					accountId: 'demo'
				});
				Manager.stop(null, identifier, done);
				conn.end();
				
			break;
			case 'status':
				identifier = new flowservices_types.FlowIdentifier({
					app: params[0],
					flow: params[1],
					version: parseInt(params[2], 10),
					accountId: 'demo'
				});
				Manager.status(null, identifier, done);
				conn.end();
				
			break;
			case 'setInstances':
				identifier = new flowservices_types.FlowIdentifier({
					app: params[0],
					flow: params[1],
					version: params[2],
					accountId: 'demo'
				});
				var flowlet_id = params[3];
				var instances = params[4];

				Manager.setInstances(null, identifier, flowlet_id, instances, function (error, response) {

					var interval = setInterval(function () {
						Manager.flowletstatus(null, identifier, flowlet_id, function (error, status) {

							if (error) {
								clearInterval(interval);
								done(error);
							} else {

								switch (status.name) {
									case 'RECONFIGURE':
									case 'ADJUSTING_RESOURCES':
									case 'LAUNCHING':
									break;
									case 'RUNNING':
										done(null, status);
										clearInterval(interval);
										conn.end();
									break;
									case 'ASK_FAILED':
										done('Failed. Could not issue additional instances.', status);
										clearInterval(interval);
										conn.end();
									break;
								}
							}
						});

					}, 500);

				});

				break;
			default:
				if (method in Manager) {
					Manager[method].apply(Manager, params.concat(done));
				} else {
					done('Unknown method for service Manager: ' + method, null);
				}
				conn.end();
		}

	};

	this.far = function (method, params, done) {

		var identifier;
		var auth_token = new flowservices_types.DelegationToken({ token: null });

		var conn = thrift.createConnection(
			this.config.upload.host,
			this.config.upload.port, {
			transport: ttransport.TFramedTransport,
			protocol: tprotocol.TBinaryProtocol
		});

		conn.on('error', function (error) {
			console.log('FARService: ', error);
			done('Could not connect to FARService');
		});

		conn.on('connect', function (error) {
			console.log(arguments);
		});
		
		var FAR = thrift.createClient(FARService, conn);

		switch (method) {

			case 'remove':

				identifier = new flowservices_types.FlowIdentifier({
					app: params[0],
					flow: params[1],
					version: -1,
					accountId: 'demo'
				});
				FAR.remove(auth_token, identifier, done);
				break;

			case 'promote':

				identifier = new flowservices_types.FlowIdentifier({
					app: params[0],
					flow: params[1],
					version: params[2],
					accountId: 'demo'
				});
				FAR.promote(auth_token, identifier, function () {
					done(arguments);
				});
				break;

			}

			conn.end();

	};

	this.monitor = function (method, params, done) {

		params = params || [];

		var conn = thrift.createConnection(
			this.config.monitor.host,
			this.config.monitor.port, {
			transport: ttransport.TFramedTransport,
			protocol: tprotocol.TBinaryProtocol
		});

		conn.on('error', function (error) {
			console.log('FlowMonitor: ', error);
			done('Could not connect to FlowMonitor.');
		});
		
		conn.on('connect', function (response) {
			var Monitor = thrift.createClient(FlowMonitor, conn);
			if (method in Monitor) {
				Monitor[method].apply(Monitor, params.concat(done));
			} else {
				done('Unknown method for service Monitor: ' + method, null);
			}

			conn.end();
			
		});

	};

	this.gateway = function (method, params, done) {

		var post_data = params.payload || "";
		var post_options = {
		host: this.config.gateway.host,
		port: this.config.gateway.port,
		path: this.config.gateway.baseUri + params.name + (params.stream ? '/' + params.stream : ''),
		method: 'POST',
		headers: {
			'com.continuuity.token': 'TOKEN',
			'Content-Length': post_data.length
		}
		};

		var post_req = http.request(post_options, function(res) {
			res.setEncoding('utf8');
			res.on('data', function (chunk) {
				console.log('Response: ' + chunk);
			});
			res.on('end', function () {
				console.log(res.statusCode);
				console.log(post_options, post_data);
			});
		});

		post_req.on('error', function (e) {
			done('Error connecting to gateway');
		});

		post_req.write(post_data);
		post_req.end();

	};

	this.upload = function (req, res, file, socket) {

		var self = this;
		var auth_token = new flowservices_types.DelegationToken({ token: null });
		var length = req.header('Content-length');

		var data = new Buffer(parseInt(length, 10));
		var idx = 0;

		console.log('Receiving file upload of length', length);

		req.on('data', function(raw) {
			raw.copy(data, idx);
			idx += raw.length;
		});

		console.log(file);

		req.on('end', function() {

			res.redirect('back');
			res.end();

			var conn = thrift.createConnection(
				self.config.upload.host,
				self.config.upload.port, {
				transport: ttransport.TFramedTransport,
				protocol: tprotocol.TBinaryProtocol
			});
			conn.on('error', function (error) {
				console.log('FARService: ', error);
				socket.emit('upload', {'error': 'Could not connect to FARService'});
			});
			
			var FAR = thrift.createClient(FARService, conn);
			FAR.init(auth_token, new flowservices_types.ResourceInfo({
				'filename': file,
				'size': data.length,
				'modtime': new Date().getTime()
			}), function (error, resource_identifier) {
				if (error) {
					console.log('FARManager init', error);
				} else {

					socket.emit('upload', {'status': 'Initialized...', 'resource_identifier': resource_identifier});
					FAR.chunk(auth_token, resource_identifier, data, function (error, result) {
						if (error) {
							console.log('FARManager chunk', error);
						} else {

							socket.emit('upload', {'status': 'Delivered...'});
							FAR.deploy(auth_token, resource_identifier, function (error, result) {
								if (error) {
									console.log('FARManager deploy', error);
								} else {

									socket.emit('upload', {'status': 'Verifying...'});

									var current_status = -1;

									var status_interval = setInterval(function () {
										FAR.status(auth_token, resource_identifier, function (error, result) {
											if (error) {
												console.log('FARManager verify', error);
											} else {

												if (current_status !== result.overall) {
													socket.emit('upload', {'status': 'verifying', 'step': result.overall, 'message': result.message, 'flows': result.verification});
													current_status = result.overall;
												}
												if (result.overall === 0 ||	// Not Found
													result.overall === 4 || // Failed
													result.overall === 5 || // Success
													result.overall === 6 || // Undeployed
													result.overall === 7) {
													clearInterval(status_interval);
												} // Else: 1 (Registered), 2 (Uploading), 3 (Verifying)
											}
										});
									}, 500);
								}
							});
						}
					});
				}
			});
		});
	};
	
}).call(exports);