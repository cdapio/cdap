//
// Monitor Simulator. Implements API found at:
// http://wiki.continuuity.com/ENG/Web+Apis/Monitor
//

var express = require('express'),
	app = express.createServer(),
	http = require('http');

app.get('/flows', function (req, res) {
	for (var i in flows) {
		flows[i].stats
	}
	res.write(JSON.stringify([
		flows[1]
	]));
});
app.get('/flows/1', function (req, res) {
	res.write(JSON.stringify(
		flows[1]
	));
});

app.listen(8083);