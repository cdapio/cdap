//
// REST Similuator. Implements API found at:
// http://wiki.continuuity.com/ENG/Web+Apis/REST
//

var express = require('express'),
	app = express.createServer(),
	http = require('http');

function make_request (uri, done) {
	var client = http.createClient(8084, '127.0.0.1');
	var request = client.request('GET', uri);
	request.on('response', function (response) {
		var data = '';
		response.setEncoding('utf8');
		response.on('data', function (chunk) {
			// TODO: Append
			data += chunk;
		});
		response.on('end', function () {
			done(data);
		});
	});
	request.end();
}

app.get('/flows', function (req, res) {
	console.log('[/flows] Forwarding to BigFlow...');
	make_request('/flows', function (result) {
		res.send(result);
	});
});
app.get('/flows/:id', function (req, res) {
	console.log('[/flows/' + req.params.id + '] Forwarding to BigFlow...');
	make_request('/flows/' + req.params.id, function (result) {
		res.send(result);
	});
});
app.get('/flows/:id/start', function (req, res) {
	console.log('[/flows/' + req.params.id + '/start] Forwarding to BigFlow...');
	make_request('/flows/' + req.params.id + '/start', function (result) {
		res.send(result);
	});
});
app.get('/flows/:id/stop', function (req, res) {
	make_request('/flows/' + req.params.id + '/stop', function (result) {
		res.send(result);
	});
});

app.listen(8082);