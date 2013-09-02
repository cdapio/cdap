/*
 * Socket.IO Mock
 */

define(['mocks/socket-router'], function (SocketRouter) {

	Em.debug('Loading Socket Mock');

	var pending = {};
	var current_id = 0;

	var eventHandlers = {
		'connect': [],
		'error': [],
		'upload': []
	};

	var getMockPath = function(request) {
	  // var path = [];
   //  path.push(request.method);
   //  for (var i=0, len=request.params.length; i<len; i++) {
   //    if (request.params[i]) {
   //      path.push(request.params[i]);
   //    }
   //  }
   //  return path.join('/');
   return request.method;
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
						"vpc_label": "SANDBOX NAME"
					}
				},
				"account": {
					"account_id": 'ABC',
					"name": 'USER NAME'
				}
			};

			eventHandlers['connect'].forEach(function (callback) {
				callback(env);
			});

		},
		request: function (service, request, callback, params) {

			request.id = current_id ++;
			var mockPath = getMockPath(request);
			var response = {};
			if (SocketRouter.hasOwnProperty(mockPath)) {
				response = SocketRouter[mockPath](request);
			}
			callback(null, response, params);
		}

	});

	Mock.reopenClass({
		type: 'Socket',
		kind: 'Mock'
	});

	return Mock;

});