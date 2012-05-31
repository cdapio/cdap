
var http = require('http');
var thrift = require('thrift');

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