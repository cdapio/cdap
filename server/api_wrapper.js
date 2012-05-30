
var http = require('http');

(function () {

	this.configure = function (host, port) {
		this.HOST = host;
		this.PORT = port;
	};

	this.request = function (method, params, done) {
		var client = http.createClient(this.PORT, this.HOST);
		var request = client.request('GET', '/' + method + '/' + (params || ''));
		request.on('response', function (response) {
			var data = [];
			response.setEncoding('utf8');
			response.on('data', function (chunk) {
				data.push(chunk);
			});
			response.on('end', function () {
				done(data.join(""));
			});
		});
		request.end();
	};

	this.subscribe = function (object, callback) {

	};

}).call(exports);