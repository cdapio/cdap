var http = require('http'),
	https = require('https'),
	thrift = require('thrift'),
	fs = require('fs'),
	log4js = require('log4js');

/**
 * Configure thrift.
 */
var AppFabricService = require('./thrift_bindings/AppFabricService.js'),
	MetricsFrontendService = require('./thrift_bindings/MetricsFrontendService.js'),
	MetadataService = require('./thrift_bindings/MetadataService.js');

var metadataservice_types = require('./thrift_bindings/metadataservice_types.js'),
	metricsservice_types = require('./thrift_bindings/metricsservice_types.js'),
	appfabricservice_types = require('./thrift_bindings/app-fabric_types.js');

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

/**
* Configure logger.
*/
var LOG_LEVEL = 'INFO';
log4js.configure({
	appenders: [
		{ type : 'console' }
	]
});
var logger = process.logger = log4js.getLogger('UI API');
logger.setLevel(LOG_LEVEL);

var LOG_DELAY = false;
setInterval(function () {
	LOG_DELAY = false;
}, 5000);


/**
 * Export API.
 */
(function () {

	this.auth = null;
	this.config = null;
	this.configure = function (config, credential) {
		this.config = config;
		this.credential = credential;
	};

	this.metadata = function (accountID, method, params, done) {

		params = params || [];

		var conn = thrift.createConnection(
			this.config['metadata.server.address'],
			this.config['metadata.server.port'], {
			transport: ttransport.TFramedTransport,
			protocol: tprotocol.TBinaryProtocol
		});
		conn.on('error', function (error) {
			logger.warn('Could not connect to MetadataService.');
			done({'fatal': 'Could not connect to MetadataService.'});
		});

		conn.on('connect', function (response) {

			var MetaData = thrift.createClient(MetadataService, conn);

			if (params.length === 2) {
				var entityType = params.shift();
				try {
					params[0] = new metadataservice_types[entityType](params[0]);
				} catch (e) {
					logger.error(method, params, e);
				}
			}

			if (method.indexOf('ByApplication') !== -1 || method === 'getFlows' || method === 'getFlow' ||
				method === 'getFlowsByStream' || method === 'getFlowsByDataset') {
				params.unshift(accountID);
			} else {
				params.unshift(new metadataservice_types.Account({
					id: accountID
				}));
			}

			if (method in MetaData) {
				try {
					MetaData[method].apply(MetaData, params.concat(done));
				} catch (e) {
					logger.warn(e);
					done(e);
				}
			} else {
				done('Unknown method for MetadataService: ' + method, null);
			}

			conn.end();
		});

	},

	this.manager = function (accountID, method, params, done) {

		params = params || [];
		params.unshift(accountID);

		if (params[4] === 'PROCEDURE') {
			params[4] = 'QUERY';
		}

		var auth_token = new appfabricservice_types.AuthToken({ token: null });

		var conn = thrift.createConnection(
			this.config['resource.manager.server.address'],
			this.config['resource.manager.server.port'], {
			transport: ttransport.TFramedTransport,
			protocol: tprotocol.TBinaryProtocol
		});

		conn.on('error', function (error) {
			if (!LOG_DELAY) {
				logger.warn('Could not connect to AppFabric (Manager).');
				done({'fatal': 'Could not connect to AppFabric (Manager).'});
				LOG_DELAY = true;
			}
		});

		conn.on('connect', function (response) {

			var Manager = thrift.createClient(AppFabricService, conn);
			var identifier = new appfabricservice_types.FlowIdentifier({
				applicationId: params[1],
				flowId: params[2],
				version: params[3] ? parseInt(params[3], 10) : -1,
				accountId: params[0],
				type: appfabricservice_types.EntityType[params[4] || 'FLOW']
			});

			switch (method) {
				case 'start':
					identifier = new appfabricservice_types.FlowDescriptor({
						"identifier": new appfabricservice_types.FlowIdentifier({
							applicationId: params[1],
							flowId: params[2],
							version: parseInt(params[3], 10),
							accountId: params[0],
							type: appfabricservice_types.EntityType[params[4] || 'FLOW']
						}),
						"arguments": params[5] || []
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

					var flowlet_id = params[4];
					var instances = params[5];

					identifier = new appfabricservice_types.FlowIdentifier({
						accountId: params[0],
						applicationId: params[1],
						flowId: params[2],
						version: params[3] || -1
					});

					Manager.setInstances(auth_token, identifier, flowlet_id, instances, done);

				break;

				default:
					if (method in Manager) {
						try {
							Manager[method].apply(Manager, params.concat(done));
						} catch (e) {
							logger.warn(e);
							done(e);
						}
					} else {
						done('Unknown method for service Manager: ' + method, null);
					}

			}

			conn.end();
		});

	};

	this.monitor = function (accountID, method, params, done) {

		params = params || [];

		var conn = thrift.createConnection(
			this.config['flow.monitor.server.address'],
			this.config['flow.monitor.server.port'], {
			transport: ttransport.TFramedTransport,
			protocol: tprotocol.TBinaryProtocol
		});

		conn.on('error', function (error) {
			logger.warn('Could not connect to MetricsService.');
			done({'fatal': 'Could not connect to MetricsService.'});
		});

		conn.on('connect', function (response) {

			var Monitor = thrift.createClient(MetricsFrontendService, conn);
			var flow, request;

			switch (method) {
				case 'getLog':

					params.unshift(accountID);
					Monitor.getLog.apply(Monitor, params.concat(done));

				break;

				case 'getLogPrev':

					params.unshift(accountID);
					Monitor.getLogPrev.apply(Monitor, params.concat(done));
					break;

				case 'getLogNext':
					params.unshift(accountID);
					Monitor.getLogNext.apply(Monitor, params.concat(done));
					break;

				case 'getCounters':
					flow = new metricsservice_types.FlowArgument({
						accountId: (params[0] === '-' ? '-' : accountID),
						applicationId: params[0],
						flowId: params[1],
						runId: params[2]
					});

					var names = params[3] || [];
					request = new metricsservice_types.CounterRequest({
						argument: flow,
						name: names
					});
					Monitor.getCounters(request, done);
				break;
				case 'getTimeSeries':

					var level = params[5] || 'FLOW_LEVEL';

					flow = new metricsservice_types.FlowArgument({
						accountId: (params[0] === '-' ? '-' : accountID),
						applicationId: params[0],
						flowId: params[1],
						flowletId: params[6] || null
					});
					request = new metricsservice_types.TimeseriesRequest({
						argument: flow,
						metrics: params[2],
						level: metricsservice_types.MetricTimeseriesLevel[level],
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

				break;
			}

			conn.end();
		});
	};

	this.far = function (accountID, method, params, done) {

		var identifier, conn, FAR,
			auth_token = new appfabricservice_types.AuthToken({ token: params[2] });

		conn = thrift.createConnection(
			this.config['resource.manager.server.address'],
			this.config['resource.manager.server.port'], {
			transport: ttransport.TFramedTransport,
			protocol: tprotocol.TBinaryProtocol
		});

		conn.on('error', function (error) {
			done({'fatal': 'Could not connect to AppFabric(FAR).'});
		});

		conn.on('connect', function (response) {

			FAR = thrift.createClient(AppFabricService, conn);

			switch (method) {

				case 'remove':

					identifier = new appfabricservice_types.FlowIdentifier({
						applicationId: params[0],
						flowId: 'NONE',
						version: -1,
						accountId: accountID
					});
					try {
						FAR.removeApplication(auth_token, identifier, done);
					} catch (e) {
						logger.error(e);
						done({'error': e});
					}
					break;

				case 'promote':

					identifier = new appfabricservice_types.ResourceIdentifier({
						applicationId: params[0],
						version: -1,
						accountId: accountID,
						resource: 'name'
					});
					try {
						FAR.promote(auth_token, identifier, params[1], done);
					} catch (e) {
						logger.error(e);
						done({'error': e});
					}

					break;

				case 'reset':

					FAR.reset(auth_token, accountID, done);

					break;

			}

			conn.end();
		});
	};

	this.gateway = function (apiKey, method, params, done, secure) {

		var post_data = params.payload || "";

		var post_options = {
			host: this.config['gateway.hostname'],
			port: this.config['gateway.port'],
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
				'X-Continuuity-ApiKey': apiKey
			}
		};

		switch (method) {
			case 'inject':

				// Adding 1000 to be picked up by nginx.
				post_options.port = parseInt(this.config['stream.rest.port'], 10) + (secure ? 1000 : 0);
				post_options.path = '/stream/' + params.stream;

			break;
			case 'query':

				// Adding 1000 to be picked up by nginx.
				post_options.port = parseInt(this.config['procedure.rest.port'], 10) + (secure ? 1000 : 0);
				post_options.path = '/procedure/' + params.app + '/' +
					params.service + '/' + params.method;

				post_data = post_data || '{}';

		}

		post_options.headers['Content-Length'] = post_data.length;

		var lib = secure ? https : http;

		var request = lib.request(post_options, function(res) {
			res.setEncoding('utf8');
			var data = [];

			res.on('data', function (chunk) {
				data.push(chunk);
			});
			res.on('end', function () {
				data = data.join('');

				done(res.statusCode !== 200 ? {
					statusCode: res.statusCode,
					response: {
						req: {
							host: post_options.host,
							port: post_options.port,
							path: post_options.path,
							data: post_data
						},
						res: {
							statusCode: res.statusCode,
							data: data
						}
					}
				} : false, {
					statusCode: res.statusCode,
					response: data
				});
			});

		});


		request.on('error', function (e) {

			done({
				statusCode: 500,
				response: e
			});

		});

		request.write(post_data);
		request.end();

	};

	this.upload = function (accountID, req, res, file, socket) {
		var self = this;
		var auth_token = new appfabricservice_types.AuthToken({ token: null });
		var length = req.header('Content-length');

		var data = new Buffer(parseInt(length, 10));
		var idx = 0;

		req.on('data', function(raw) {
			raw.copy(data, idx);
			idx += raw.length;
		});

		req.on('end', function() {

			res.write('');
			res.end();

			logger.trace('Upload received.', file + '(' + data.length + ')');

			var conn = thrift.createConnection(
				self.config['resource.manager.server.address'],
				self.config['resource.manager.server.port'], {
				transport: ttransport.TFramedTransport,
				protocol: tprotocol.TBinaryProtocol
			});
			conn.on('error', function (error) {
				logger.error('Could not connect to AppFabricService (Upload).', error);
				socket.emit('upload', {'status': 'failed', 'step': 4, 'message': 'Could not connect to AppFabricService'});
			});

			conn.on('connect', function (response) {

				var FAR = thrift.createClient(AppFabricService, conn);
				FAR.init(auth_token, new appfabricservice_types.ResourceInfo({
					'accountId': accountID,
					'applicationId': 'nil',
					'filename': file,
					'size': data.length,
					'modtime': new Date().getTime()
				}), function (error, resource_identifier) {
					if (error) {
						logger.warn('AppFabric Init', error);
						socket.emit('upload', {'status': 'failed', 'step': 4, 'message': error.name + ': ' + error.message });

					} else {

						logger.trace('Upload to AppFabric initialized.');

						socket.emit('upload', {'status': 'Initialized...', 'resource_identifier': resource_identifier});

						var send_deploy = function () {

							socket.emit('upload', {'status': 'Deploying...'});

							FAR.deploy(auth_token, resource_identifier, function (error, result) {
								if (error) {
									logger.warn('FARManager deploy', error);
									socket.emit('upload', {'status': 'failed', 'step': 4, 'message': error.name + ': ' + error.message });
								} else {
									socket.emit('upload', {step: 0, 'status': 'Verifying...', result: arguments});

									var current_status = -1;

									var status_interval = setInterval(function () {
										FAR.dstatus(auth_token, resource_identifier, function (error, result) {
											if (error) {
												logger.warn('FARManager verify', error);
												socket.emit('upload', {'status': 'failed', 'step': 4, 'message': error.name + ': ' + error.message });
											} else {

												logger.trace('Upload status changed', result);

												if (current_status !== result.overall) {
													socket.emit('upload', {'status': 'verifying', 'step': result.overall, 'message': result.message, 'flows': result.verification});
													current_status = result.overall;
												}
												if (result.overall === 0 ||	// Not Found
													result.overall === 4 || // Failed
													result.overall === 5 || // Success
													result.overall === 6 || // Undeployed
													result.overall === 7) {

													conn.end();

													clearInterval(status_interval);
												} // 1 (Registered), 2 (Uploading), 3 (Verifying)
											}
										});
									}, 500);
								}
							});

						};

						var send_chunk = function (index, size) {

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
						};

						var CHUNK_SIZE = 102400;

						send_chunk(0, CHUNK_SIZE > data.length ? data.length : CHUNK_SIZE);

					}
				});
			});
		});
	};

}).call(exports);