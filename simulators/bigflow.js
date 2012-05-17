//
// BigFlow Simulator. Implements API found at:
// http://wiki.continuuity.com/ENG/Web+Apis/BigFlowSimulator
//

var express = require('express'),
	app = express.createServer();

var flowlets = {
	1: {
		id: 1,
		name: 'Detect Language',
		tuplesIn: 0
	}, 
	2: {
		id: 2,
		name: 'Write to DB',
		tuplesIn: 0
	}
};

var flows = {
	1: {
		id: 1,
		name: 'Tweet Rate Clocker',
		status: 'stopped',
		flowlets: [
			flowlets[1],
			flowlets[2]
		],
		runs: 0,
		started: null,
		stopped: null,
		instances: [],
		history: [],
		stats: {
			'0:1': 0,
			'1:2': 0
		}
	},
	2: {
		id: 2,
		name: 'Recommended Users',
		status: 'stopped',
		flowlets: [
			flowlets[1],
			flowlets[2]
		],
		runs: 0,
		started: null,
		stopped: null,
		instances: [],
		history: [],
		stats: {
			'0:1': 0,
			'1:2': 0
		}
	},
	3: {
		id: 3,
		name: 'Top Tweeted URLs',
		status: 'error',
		flowlets: [
			flowlets[1],
			flowlets[2]
		],
		runs: 0,
		started: null,
		stopped: null,
		instances: [],
		history: [],
		stats: {
			'0:1': 0,
			'1:2': 0
		}
	}
};

function toISO8601(date) {
    var pad_two = function(n) {
        return (n < 10 ? '0' : '') + n;
    };
    var pad_three = function(n) {
        return (n < 100 ? '0' : '') + (n < 10 ? '0' : '') + n;
    };
    return [
        date.getUTCFullYear(),
        '-',
        pad_two(date.getUTCMonth() + 1),
        '-',
        pad_two(date.getUTCDate()),
        'T',
        pad_two(date.getUTCHours()),
        ':',
        pad_two(date.getUTCMinutes()),
        ':',
        pad_two(date.getUTCSeconds()),
        '.',
        pad_three(date.getUTCMilliseconds()),
        'Z',
    ].join('');
}

function start (id) {
	console.log('STARTING', id);

	flows[id].status = 'running';
	flows[id].started = toISO8601(new Date());
	flows[id].runs ++;
	flows[id].history.push({
		action: 'start',
		time: toISO8601(new Date()),
		user: 'demouser',
		instance: 'Instance 1'
	});

	var interval = setInterval(function () {

		if (flows[id].status == 'stopping') {
			clearInterval (interval);
			flows[id].status = 'stopped';
			flows[id].stats = {};
		} else {
			var k = 0;
			for (var i in flows[id].flowlets) {
				k++;
				flows[id].flowlets[i].tuplesIn += Math.round(Math.ceil(Math.random() * 100) / k);
			}
		}

	}, 250);
}
function stop (id) {
	console.log('STOPPING', id);
	flows[id].stopped = toISO8601(new Date());
	flows[id].status = 'stopping';

	flows[id].history.push({
		action: 'stop',
		time: toISO8601(new Date()),
		user: 'demouser',
		instance: 'Instance 1'
	});

	for (var i in flows[id].flowlets) {
		flows[id].flowlets[i].tuplesIn = 0;
	}

}

app.get('/flows', function (req, res) {
	console.log('[/flows] Responding...');
	res.send(JSON.stringify([
		flows[1], flows[2], flows[3]]));
});
app.get('/flows/:id', function (req, res) {
	console.log('[/flows/' + req.params.id + '] Responding...');
	res.send(JSON.stringify(
		flows[req.params.id]
	));
});
app.get('/flows/:id/start', function (req, res) {
	console.log('[/flows/' + req.params.id + '/start] Responding...');
	start(req.params.id);
	res.send(true);
});
app.get('/flows/:id/stop', function (req, res) {
	console.log('[/flows/' + req.params.id + '/stop] Responding...');
	stop(req.params.id);
	res.send(true);
});

app.listen(8084);