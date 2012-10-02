
var http = require('http'),
	thrift = require('thrift'),
	fs = require('fs');

var FARService = require('./thrift_bindings/FARService.js'),
	MetricsFrontendService = require('./thrift_bindings/MetricsFrontendService.js'),
	metricsservice_types = require('./thrift_bindings/metricsservice_types.js');
var FlowService = require('./thrift_bindings/FlowService.js'),
	flowservices_types = require('./thrift_bindings/flowservices_types.js');
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

		// Pushing the accountID into arguments. Comes from the authenticated session.
		params.unshift('demo');

		var auth_token = new flowservices_types.DelegationToken({ token: null });

		var conn = thrift.createConnection(
			this.config['flow.manager.server.address'],
			this.config['flow.manager.server.port'], {
			transport: ttransport.TFramedTransport,
			protocol: tprotocol.TBinaryProtocol
		});

		conn.on('error', function (error) {
			console.log('FlowManager: ', error);
			done('Could not connect to FlowMonitor.');
		});
		
		var Manager = thrift.createClient(FlowService, conn);
		var identifier = new flowservices_types.FlowIdentifier({
			app: params[1],
			flow: params[2],
			version: params[3] ? parseInt(params[3], 10) : -1,
			accountId: params[0]
		});

		switch (method) {
			case 'start':
				identifier = new flowservices_types.FlowDescriptor({
					identifier: new flowservices_types.FlowIdentifier({
						app: params[1],
						flow: params[2],
						version: parseInt(params[3], 10),
						accountId: 'demo'
					}),
					"arguments": []
				});
				Manager.start(auth_token, identifier, done);
			break;
			case 'stop':
				Manager.stop(auth_token, identifier, done);
			break;
			case 'status':
				Manager.status(auth_token, identifier, done);
			break;
			case 'getFlowDefinition':
				Manager.getFlowDefinition(identifier, done);
			break;
			case 'getFlowHistory':
				Manager.getFlowHistory(identifier, done);
			break;
			case 'setInstances':

				var flowlet_id = params[3];
				var instances = params[4];

				Manager.setInstances(auth_token, identifier, flowlet_id, instances, done);

				/*
					function (error, response) {

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
				*/

				break;

			case 'getAccountSummary':
				done(false, {
					health: 'OK',
					errors: 0,
					counts: {
						apps: 1,
						flows: 1,
						streams: 1,
						datasets: 2
					}
				});
				break;

			case 'getApp':
				done(false, {
					health: 'OK',
					errors: 0,
					type: 'App',
					id: 'CountRandom',
					name: 'Recommendation Engine',
					counts: {
						flows: 2,
						streams: 1,
						datasets: 2
					}
				});
				break;
			case 'getApps':

				done(false, [{
					health: 'OK',
					errors: 0,
					type: 'App',
					id: 'CountRandom',
					name: 'Recommendation Engine',
					counts: {
						flows: 1,
						streams: 1,
						datasets: 2
					}
				}, {
					health: 'OK',
					errors: 0,
					type: 'App',
					id: 'End2End',
					name: 'Analytics App',
					counts: {
						flows: 2,
						streams: 0,
						datasets: 4
					}
				}]);

				break;
			case 'getFlows':
			case 'getFlowsForApp':
				Manager[method].apply(Manager, params.concat(done));
				break;
			case 'getStreams':
			case 'getStreamsForApp':

				done(false, [{
					health: 'OK',
					errors: 0,
					id: '1234',
					name: 'User Data Input'
				}]);

				break;
			case 'getDataSets':
			case 'getDataSetsForApp':

				done(false, [{
					health: 'OK',
					errors: 0,
					id: '1234',
					name: 'Big Data'
				}, {
					health: 'OK',
					errors: 0,
					id: '5678',
					name: 'User Data'
				}]);

				break;
			default:
				if (method in Manager) {
					try {
						Manager[method].apply(Manager, params.concat(done));
					} catch (e) {
						console.log(e);
						done(e);
					}
				} else {
					done('Unknown method for service Manager: ' + method, null);
				}
				
		}

		conn.end();

	};

	this.far = function (method, params, done) {

		var identifier;
		var auth_token = new flowservices_types.DelegationToken({ token: null });

		var conn = thrift.createConnection(
			this.config['resource.manager.server.address'],
			this.config['resource.manager.server.port'], {
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
					version: params[2],
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
				FAR.promote(auth_token, identifier, done);

				break;

			}

			conn.end();

	};

	this.monitor = function (method, params, done) {

		params = params || [];

		var conn = thrift.createConnection(
			this.config['flow.monitor.server.address'],
			this.config['flow.monitor.server.port'], {
			transport: ttransport.TFramedTransport,
			protocol: tprotocol.TBinaryProtocol
		});

		conn.on('error', function (error) {
			console.log('FlowMonitor: ', error);
			done('Could not connect to FlowMonitor.');
		});
		
		conn.on('connect', function (response) {
			var Monitor = thrift.createClient(MetricsFrontendService, conn);

			switch (method) {
				case 'getCounters':
					var flow = new metricsservice_types.FlowArgument({
						accountId: 'demo',
						applicationId: params[0],
						flowId: params[1]
					});
					var request = new metricsservice_types.CounterRequest({
						argument: flow
					});
					Monitor.getCounters(request, done);
				break;
				case 'getTimeSeries':

					var level = params[5] || 'FLOW_LEVEL';

					var flow = new metricsservice_types.FlowArgument({
						accountId: 'demo',
						applicationId: params[0],
						flowId: params[1],
						flowletId: params[6] || null
					});
					var request = new metricsservice_types.TimeseriesRequest({
						argument: flow,
						metrics: params[2],
						level: metricsservice_types.MetricTimeseriesLevel[level],
						endts: params[4],
						startts: params[3]
					});
					Monitor.getTimeSeries(request, function (error, response) {

						if (error) {
							done(true, false);
							return;
						}

						// Nukes timestamps since they're in Buffer format
						for (var metric in response.points) {

							var res = response.points[metric];
							if (res) {
								var j = res.length;
								while(j--) {
									res[j].timestamp = 0;
								}
								response.points[metric] = res;
							}
						}

						done(error, response);

					});
					console.log('done');
				break;
			}

			conn.end();
			
		});

	};

	this.gateway = function (method, params, done) {

		var post_data = params.payload || "";
		var post_options = {
		host: this.config['gateway.hostname'],
		port: this.config['gateway.port'],
		path: '/rest-stream/' + params.name + (params.stream ? '/' + params.stream : ''),
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
			done(e);
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
				self.config['resource.manager.server.address'],
				self.config['resource.manager.server.port'], {
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

					function send_deploy () {

						socket.emit('upload', {'status': 'Deploying...'});

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

					function send_chunk (index, size) {

						FAR.chunk(auth_token, resource_identifier, data.slice(index, index + size), function () {

							if (error) {
								socket.emit('Chunk error');
							} else {

								if (index + size === data.length) {
									send_deploy();

								} else {
									var length = size;
									if (index + (size * 2) > data.length) {
										length = data.length % size;
									}
									send_chunk(index + size, length);
								}
							}
						});
					}

					var CHUNK_SIZE = 102400;
					send_chunk(0, CHUNK_SIZE > data.length ? data.length : CHUNK_SIZE);

				}
			});
		});
	};
	
}).call(exports);