/*
 * HTTP Resource
 */

define([], function () {

	Em.debug('Loading HTTP Resource');

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

	var Resource = Em.Object.extend({

		get: function () {

			var path = findPath(arguments);
			var callback = findCallback(arguments);

			$.getJSON(path, callback).fail(function (req) {

				var error = JSON.parse(req.responseText);
				if (error.fatal) {

					$('#warning').html('<div>' + error.fatal + '</div>').show();

				}

			});

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

			$.ajax({
				url: path,
				data: JSON.stringify(object),
				type: "POST",
				contentType: "application/json"
			}).done(function (response) {

				if (response.error && response.error.fatal) {
					$('#warning').html('<div>' + response.error.fatal + '</div>').show();
				} else {
					callback(response.result, response.error);
				}

			}).fail(function (xhr) {

				$('#warning').html('<div>Encountered a connection problem.</div>').show();

			});

		},

		rpc: function () {

			var args = [].slice.call(arguments);
			args.unshift('rpc');

			var object = args[args.length - 2];

			if (typeof object === 'object' && object.length) {
				args[args.length - 2] = object; //{ 'params[]': JSON.stringify(object) };
			}

			this.post.apply(this, args);

		},

		"delete": function () {

			var path = findPath(arguments);
			var callback = findCallback(arguments);
			this.post(path + '?_method=DELETE', callback);

		}

	});

	Resource.reopenClass({
		type: 'HTTP',
		kind: 'Resource'
	});

	return Resource;

});