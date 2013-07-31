/*
 * HTTP Mock
 */

define(['mocks/results/elements', 'mocks/results/metrics/timeseries',
	'mocks/results/metrics/counters', 'mocks/results/metrics/samples',
	'mocks/results/rpc', 'mocks/http-router'],
	function (Elements, TimeSeries, Counters, Samples, RPC, HttpRouter) {

	Em.debug('Loading HTTP Mock');

	/*
	 * Joins the arguments as a path string.
	 * e.g. HTTP.get('metrics', 1, 2, 3) => GET /metrics/1/2/3 HTTP/1.1
	 */
	function findPath(args) {
		var path = [];
		for (var i = 0; i < args.length; i ++) {
			if (typeof args[i] === 'string') {
				path.push(args[i]);
			}
		}
		return '/' + path.join('/');
	}

	/*
	 * Finds the object argument based on the last argument.
	 */
	function findObject(args) {
		var object = args[args.length - 1];
		if (typeof object === 'function') {
			object = args[args.length - 2];
		}
		return (!object || typeof object === 'string' ? null : object);
	}

	/*
	 * Finds the callback argument based on the last argument.
	 */
	function findCallback(args) {
		var callback = args[args.length - 1];
		return (typeof callback === 'function' ? callback : function () {
			Em.debug('No callback provided for HTTP response.');
		});
	}

	var Mock = Em.Object.extend({

		get: function () {

			var path = findPath(arguments);
			var callback = findCallback(arguments);
			var result = HttpRouter.getResult(path);

			callback(result, 200);

		},

		rest: function () {

			var args = [].slice.call(arguments);
			args.unshift('rest');
			this.get.apply(this, args);

		},

		post: function () {

			var path = findPath(arguments);
			var object = findObject(arguments);
			var callback = findCallback(arguments);
			var response = [];


			if (path === '/metrics') {

				if (typeof object === 'object' && object.length) {
					var path, query;
					for (var i = 0; i < object.length; i ++) {

						path = object[i].split('?')[0];
						query = C.Util.parseQueryString(object[i]);

						if (query.count) {

							TimeSeries(path, query, function (status, metricsResult) {
								response.push(metricsResult);
							});

						} else if (query.aggregate) {

							Counters(path, query, function (status, metricsResult) {
								response.push(metricsResult);
							});

						} else if (query.summary) {

							Summary(path, query, function (status, metricsResult) {
								response.push(metricsResult);
							});

						}

					}
					callback({
						error: null,
						result: response
					}, 200);

				} else {
					callback({
						error: 1,
						result: null
					}, 500);
				}

			} else {

				var response = HttpRouter.getResult(path);
				callback({
					result: response,
					error: null
				}, 200);

			}

		},

		rpc: function () {

			var args = [].slice.call(arguments);
			args.unshift('rpc');

			var object = args[args.length - 2];

			this.post.apply(this, args);

		},

		"delete": function (path) {

			var path = findPath(arguments);
			var callback = findCallback(arguments);
			this.post(path + '?_method=DELETE', callback);

		}

	});

	Mock.reopenClass({
		type: 'HTTP',
		kind: 'Mock'
	});

	return Mock;

});