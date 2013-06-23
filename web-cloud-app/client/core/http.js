/*
 * HTTP Resource
 */

define([], function () {

	Em.debug('Loading HTTP Resource');

	/*
	 * Joins the arguments as a path string.
	 * e.g. HTTP.get('metrics', 1, 2, 3) => GET /metrics/1/2/3 HTTP/1.1
	 */
	function getPath(args) {
		var i = args.length, path = [];
		while (i--) {
			if (typeof args[i] === 'string') {
				path.push(args[i]);
			}
		}
		return path.join('/');
	}

	/*
	 * Finds the object argument based on the last argument.
	 */
	function getObject(args) {
		var object = args[args.length - 1];
		return (!object || typeof object === 'string' ? null : object);
	}

	/*
	 * Finds the callback argument based on the last argument.
	 */
	function getCallback(args) {
		var callback = args[args.length - 1];
		return (typeof callback === 'function' ? callback : function () {
			Em.debug('No callback provided for HTTP response.');
		});
	}

	var Resource = Em.Object.extend({

		get: function () {

			var path = getPath(arguments);
			var callback = getCallback(arguments);

			$.ajax({
				url: path,
				success: callback,
				statusCode: {
					404: function() {
						callback(null, 404);
					}
				}
			});

		},

		put: function () {

			var path = getPath(arguments);
			var object = getObject(arguments);

			return {
				status: 200,
				result: {
					id: '',
					type: ''
				}
			};

		},

		post: function () {

			var path = getPath(arguments);
			var object = getObject(arguments);

			return {
				status: 200,
				result: {
					id: '',
					type: ''
				}
			};

		},

		"delete": function () {

			var path = getPath(arguments);

			return {
				status: 200,
				result: null
			};

		},

		__methodNames: {
			'App': 'getApplications',
			'Flow': 'getFlows',
			'Stream': 'getStreams',
			'Procedure': 'getQueries',
			'Dataset': 'getDatasets'
		},

		getElements: function (type, callback, appId, arg) {

			var self = this;

			C.get('metadata', {
				method: this.__methodNames[type] + (appId ? 'ByApplication' : ''),
				params: appId ? [appId] : []
			}, function (error, response, params) {

				if (error) {
					if (typeof callback === 'function') {
						callback([], arg);
					} else {
						C.interstitial.label(error);
					}
				} else {
					var objects = response.params || [];
					var i = objects.length, type = params[0];

					while (i--) {
						objects[i] = C[type].create(objects[i]);
					}

					if (typeof params[1] === 'function') {
						callback(objects, arg);
					}
				}
			}, [type, callback]);

		},

		getMetrics: function () {

		}
	});

	Resource.reopenClass({
		type: 'HTTP',
		kind: 'Mock'
	});

	return Resource;

});