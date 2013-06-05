/*
 * Socket.IO Mock
 */

define([], function () {

	Em.debug('Loading Socket Mock');

	var pending = {};
	var current_id = 0;

	var eventHandlers = {
		'connect': [],
		'error': [],
		'upload': []
	};

	var Mock = Em.Object.extend({

		on: function (event, callback) {

			if (undefined === eventHandlers[event]) {
				eventHandlers[event] = [];
			}

			eventHandlers[event].push(callback);

		},

		connect: function () {

			var env = {
				"product": "Sandbox",
				"location": "remote",
				"version": 'CLIENT TEST',
				"ip": 'NO IP',
				"cluster": {
					"info": {
						"vpc_label": "MOSITES SANDBOX"
					}
				},
				"account": {
					"account_id": 'ABC',
					"name": 'DONALD'
				}
			};

			eventHandlers['connect'].forEach(function (callback) {
				callback(env);
			});

		},
		request: function (service, request, response, params) {

			request.id = current_id ++;
			pending[request.id] = [response, params, new Date().getTime()];

			var response = {
				id: request.id
			};

			pending[response.id][0](null, {}, pending[response.id][1]);
			delete pending[response.id];

		}

	});

	Mock.reopenClass({
		type: 'Socket',
		kind: 'Mock'
	});

	return Mock;

});