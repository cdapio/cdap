
var http = require('http'),
	thrift = require('thrift'),
	fs = require('fs');

var FARService = require('./gen-nodejs/FARService.js'),
	FlowService = require('./gen-nodejs/FlowService.js'),
	flowservices_types = require('./gen-nodejs/flowservices_types.js');

var ttransport;
try {
ttransport = require('thrift/lib/thrift/transport');
} catch (e) {
ttransport = require('thrift/transport');
}

var tprotocol;
try {
tprotocol = require('thrift/lib/thrift/protocol');
} catch (e) {
tprotocol = require('thrift/protocol');
}

(function () {

	this.configure = function (host, port) {

		/*
		this.connection = thrift.createConnection(host, port);
		this.connection.on('error', function (error) {
			console.log('THRIFT ERROR', error);
		});

		this.FlowService = thrift.createClient(FlowService, this.connection);
	
		 Subscriber.start();
		*/

	};

	this.request = function (method, params, done) {
		
		done(true);

		return;

		if (method in this.Manager) {
			done('Unknown method for service Manager: ' + method, null);
		} else {
			this.Manager[method].apply(this.Manager, params.concat(done));
		}

	};

	this.status = function (params, done) {

		this.Monitor.getFlowStatus(params.concat(done));

	};

	this.upload = function (req, res, socket) {

		console.log('Receiving Upload');

		var auth_token = new flowservices_types.DelegationToken({ token: null });

		var body = null;
		var header = '';

		var content_type = req.headers['content-type'];
		var content_length = parseInt(req.headers['content-length'], 10);
		var headerFlag = true;
		var filename = 'upload.jar';
		var filenameRegexp = /filename="(.*)"/m;
		console.log('content-type: ' + content_type);
		console.log('content-length: ' + content_length);

		req.on('data', function(raw) {
			console.log('received data length: ' + raw.length);

			if (body === null) {
				body = raw;
			} else {
				body += raw;
			}

		});

		req.on('end', function() {

			console.log('final file size: ' + body.length);

			var conn = thrift.createConnection('127.0.0.1', 45000, {
				transport: ttransport.TFramedTransport,
				protocol: tprotocol.TBinaryProtocol
			});
			conn.on('error', function (error) {
				console.log('FARService: THRIFT ERROR', error);
			});
			
			var FAR = thrift.createClient(FARService, conn);
			FAR.init(auth_token, new flowservices_types.ResourceInfo({
				'filename': filename,
				'size': body.length,
				'modtime': new Date().getTime()
			}), function (error, resource_identifier) {
				if (error) {

				} else {

					socket.emit('upload', resource_identifier);

					FAR.chunk(auth_token, resource_identifier, body, function (error, result) {
						if (error) {

						} else {

							socket.emit('upload', 'sent');

							console.log('Chunk result', error, result);						
							FAR.deploy(auth_token, resource_identifier, function (error, result) {
								if (error) {
									console.log(error);
								} else {

									socket.emit('upload', 'verifying');

//									var FAR_STATUS = {
//										0: 'Not found',
//										1: 'Registered',
//										2: 'Uploading',
//										3: 'Verifying',
//										4: 'Failed',
//										5: 'Success',
//										6: 'Undeployed'
//									};

									var status_interval = setInterval(function () {

										FAR.status(auth_token, resource_identifier, function (error, result) {

											if (error) {
												console.log(error);
											} else {
												if (result.overall === 0 ||
													result.overall === 4 ||
													result.overall === 5 ||
													result.overall === 6) {

													socket.emit('upload', result);
													clearInterval(status_interval);
												}
											}
										});

									}, 100);

									console.log('Deploy result', error, result);

								}
							});

						}
					});
			
				}
			});

			console.log('done');
			res.redirect('back');
		});
	};
	/*
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
	*/
}).call(exports);