/*
 * HTTP Resource
 */

define([], function () {

	Em.debug('Loading HTTP Resource');

	var AJAX_TIMEOUT = 30000;

	var Resource = Em.Object.extend({

		// Callable methods on HTTP resource.
		get: function () {

			var path = this.findPath(arguments);
			var queryString = this.findQueryString(arguments);
			var callback = this.findCallback(arguments);
			path = queryString ? path + '?' + queryString : path;

			var cacheData = queryString.indexOf('cache=true') !== -1;

			if (C.ENABLE_CACHE) {
				var cacheResult = C.SSAdapter.find(path);

				if (cacheData && cacheResult) {

					callback(cacheResult);
					return;

				} else {

					this.getJSON(path, callback, cacheData);

				}
			} else {
				this.getJSON(path, callback);
			}


		},

		getJSON: function (path, callback, cacheData) {
      var self = this;

			$.getJSON(path, function (result, textStatus, jqXhr) {

				if (cacheData) {
					C.SSAdapter.save(path, result);
				}
				callback(result, jqXhr.status);

			}).fail(function (xhr, status, error) {

				var error = xhr.responseText || '';

				if (error) {

          C.Util.showWarning(error);

				} else {

          C.Util.showWarning('Encountered a connection problem.');

				}
				callback(error, xhr.status);

			});
		},

		rest: function () {
      var self = this;

			var args = [].slice.call(arguments);
			args.unshift('rest');
			this.get.apply(this, args);

		},

		post: function () {
      var self = this;

			var path = this.findPath(arguments);
			var object = this.findObject(arguments);
			var callback = this.findCallback(arguments);
			var options = {
				url: path,
				type: 'POST',
				timeout: AJAX_TIMEOUT
			};

			if (!$.isEmptyObject(object)) {
				options['data'] = JSON.stringify(object);
				options['contentType'] = 'application/json';
			}
			$.ajax(options).done(function (response, statusText, xhr) {
				if (response.error && response.error.fatal) {
          C.Util.showWarning(response.error.fatal);
				} else {
					callback(response, xhr.status, statusText);
				}

			}).fail(function (xhr, status, error) {
				callback({ response: null, error: error, message: xhr.responseText }, xhr.status, error);
			});

		},

		rpc: function () {
      var self = this;

			var args = [].slice.call(arguments);
			if (args[0].indexOf('/') === 0) {
				args[0] = args[0].slice(1);
			}

			args.unshift('rest');
			this.post.apply(this, args);

		},

		put: function () {
      var self = this;

			var path = this.findPath(arguments);
			var object = this.findObject(arguments);
			var callback = this.findCallback(arguments);

			var options = {
				url: path,
				type: 'PUT',
				timeout: AJAX_TIMEOUT
			};

			if (!$.isEmptyObject(object)) {
				options['data'] = JSON.stringify(object);
				options['contentType'] = 'application/json';
			}
			$.ajax(options).done(function (response, status) {

				if (response.error && response.error.fatal) {
          C.Util.showWarning(response.error.fatal);
				} else {
					callback(response, status);
				}

			}).fail(function (xhr, status, error) {
				callback(error, status);
			});

		},

		del: function () {
      var self = this;

			var path = this.findPath(arguments);
			var callback = this.findCallback(arguments);
			var options = {
				url: path,
				type: 'DELETE',
				timeout: AJAX_TIMEOUT
			};
			$.ajax(options).done(function (response, status) {

				if (C.ENABLE_CACHE) {
					// Delete cache as it would become stale upon deletion.
					C.SSAdapter.clear();
				}

				if (response.error) {
          C.Util.showWarning(response.error.fatal);
				} else {
					callback(response, status);
				}
			}).fail(function (xhr, status, error) {
				callback(error, xhr.responseText);
			});

		},

		/*
		 * Joins the arguments as a path string.
		 * e.g. HTTP.get('metrics', 1, 2, 3) => GET /metrics/1/2/3 HTTP/1.1
		 */
		findPath: function(args) {
			var path = [];
			for (var i = 0; i < args.length; i ++) {
				if (typeof args[i] === 'string' || typeof args[i] === 'number') {
					path.push(args[i]);
				}
			}
			return '/' + path.join('/');
		},

		/**
		 * Iterates over arguments to find an object that should be mapped as a query string. This is not
		 * recursive and reaches only 1 level depth of recursion.
		 * eg: HTTP.get('metrics', 1, 2, {'count': 'total', 'foo': 'bar'}) => count=total&foo=bar
		 * @param {Array} args arguments.
		 * @returns {string} query string part of url.
		 */
		findQueryString: function(args) {
			var query = {};

			args = Array.prototype.slice.call(args);
			for (var i = 0, len = args.length; i < len; i++) {
				if(Object.prototype.toString.call(args[i]) === "[object Object]") {
					$.extend(query, args[i]);
				}
			}

			return $.param(query);
		},

		/*
		 * Finds the object argument based on the last argument.
		 */
		findObject: function(args) {
			var object = args[args.length - 1];
			if (typeof object === 'function') {
				object = args[args.length - 2];
			}
			return object instanceof Object ? object : null;
		},

		/*
		 * Finds the callback argument based on the last argument.
		 */
		findCallback: function(args) {
			var callback = args[args.length - 1];
			return (typeof callback === 'function' ? callback : function () {
				Em.debug('No callback provided for HTTP response.');
			});
		}

	});

	Resource.reopenClass({
		type: 'HTTP',
		kind: 'Resource'
	});

	return Resource;

});