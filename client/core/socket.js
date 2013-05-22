/*
 * Socket.IO
 */

define([], function () {

	Em.debug('Loading Socket');

	var pending = {};
	var current_id = 0;

	var warningTimeout;
	var toAverage = [];
	var averageOver = 30;
	var maxResponseTime = 5000;

	var connectedHandlers = [];
	var errorHandlers = [];

	var socket;

	var eventHandlers = {
		'upload': []
	};

	function connected (env) {

		connectedHandlers.forEach(function (callback) {
			callback(env);
		});

	}

	function error (env) {

		errorHandlers.forEach(function (callback) {
			callback(env);
		});

	}

	var Socket = Em.Object.extend({

		addConnectedHandler: function (callback) {

			connectedHandlers.push(callback);

		},

		addErrorHandler: function (callback) {

			errorHandlers.push(callback);

		},

		addEventHandler: function (event, callback) {

			if (undefined === eventHandlers[event]) {
				eventHandlers[event] = [];
			}

			eventHandlers[event].push(callback);

		},

		connect: function () {

			var self = this;

			socket = io.connect(document.location.hostname, {
				secure: document.location.protocol === 'https:'
			});

			socket.on('exec', function (err, response) {

				if (err && err.fatal) {

					error(err.fatal);
					delete pending[response.id];
					return;

				}

				if (pending[response.id] &&
					typeof pending[response.id][0] === 'function') {

					if (self.get('watchLatency')) {

						toAverage.push(new Date().getTime() - pending[response.id][2]);

						if (toAverage.length > averageOver) {
							toAverage.shift();
						}

						var i = toAverage.length, sum = 0;
						while (i--) {
							sum += toAverage[i];
						}

						if(sum / toAverage.length > maxResponseTime) {

							clearTimeout(warningTimeout);
							$('#warning').fadeIn();
							warningTimeout = null;

						} else {

							if (warningTimeout === null) {
								warningTimeout = setTimeout(function () {
									$('#warning').fadeOut();
									warningTimeout = null;
								}, 1000);
							}
						}
					}

					pending[response.id][0](err, response, pending[response.id][1]);
					delete pending[response.id];
				}

			});
			socket.on('failure', function (failure) {
				error(failure);
			});
			socket.on('upload', function (response) {

				for (var i = 0; i < eventHandlers['upload'].length; i ++) {
					eventHandlers['upload'][i](response);
				}

			});
			socket.on('error', function () {
				error('Error', arguments);
			});
			socket.on('connect_failed', function () {
				error('Connection failed.', arguments);
			});
			socket.on('reconnect_failed', function () {
				error('Reconnect failed.', arguments);
			});
			socket.on('reconnect', function () {
				error('Reconnected.', arguments);
			});
			socket.on('reconnecting', function (timeout, attempt) {
				error('Disconnected. Attempting to reconnect. (' + attempt + ')', arguments);
			});
			socket.on('env', connected);

		},
		request: function (service, request, response, params) {

			if (!service) {
				return;
			}
			request.id = current_id ++;
			pending[request.id] = [response, params, new Date().getTime()];
			socket.emit(service, request);

		}

	});

	return Socket.create();

});