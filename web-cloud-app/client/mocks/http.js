/*
 * HTTP Mock
 */

define(['mocks/results/elements', 'mocks/results/metrics/counters',
	'mocks/results/rpc', 'mocks/http-router'],
	function (Elements, Counters, RPC, HttpRouter) {

	Em.debug('Loading HTTP Mock');

	/*
	 * Joins the arguments as a path string.
	 * e.g. HTTP.get('metrics', 1, 2, 3) => GET /metrics/1/2/3 HTTP/1.1
	 */
	function getPath(args) {
		var path = [];
		for (var i = 0; i < args.length; i ++) {
			if (typeof args[i] === 'string') {
				path.push(args[i]);
			}
		}
		return '/' + path.join('/');
	}

	/*
	 * Finds the object argument based on the second to last argument.
	 */
	function getObject(args) {
		var object = args[args.length - 2];
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

	var Mock = Em.Object.extend({

		"get": function () {

			var path = getPath(arguments);
			var callback = getCallback(arguments);
			var status = 200;
			var result = HttpRouter.getResult(path);

			callback(status, result);

		},

		"put": function () {

			var path = getPath(arguments);
			var object = getObject(arguments);
			var callback = getCallback(arguments);

			callback(500, null);

		},

		/*
		 * RPC is executed via POST.
		 */
		"post": function () {

			var path = getPath(arguments);
			var object = getObject(arguments);
			var callback = getCallback(arguments);
			var result = [];

			if (path === '/metrics') {

				if (typeof object === 'object' && object.length) {
					var path, query;
					for (var i = 0; i < object.length; i ++) {

						path = object[i].split('?')[0];
						query = C.Util.parseQueryString(object[i]);

						if (query.count) {
							TimeSeries(path, query, function (status, metricsResult) {
								result.push(metricsResult);
							});
						} else if (query.total) {
							Counters(path, query, function (status, metricsResult) {
								result.push(metricsResult);
							});
						} else if (query.summary) {
							Summary(path, query, function (status, metricsResult) {
								result.push(metricsResult);
							});
						}

					}
					callback(200, result);

				} else {
					callback(500, null);
				}

			} else {
				callback(500, null);
			}

		},

		"delete": function (path) {

			var path = getPath(arguments);

			return {
				status: 200,
				result: null
			};

		},

		"getElements": function (type, callback, appId, arg) {

			var objects = $.extend(true, [], Elements[type].result);
			var params = [type, callback];

			var i = objects.length, type = params[0];

			while (i--) {
				objects[i] = C[type].create(objects[i]);
			}

			if (typeof params[1] === 'function') {
				callback(objects, arg);
			}

		}

	});

	Mock.reopenClass({
		type: 'HTTP',
		kind: 'Mock'
	});

	return Mock;

});