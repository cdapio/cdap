
var http = require('http');

(function () {

	this.configure = function (host, port) {
		this.HOST = host;
		this.PORT = port;
	};

	var flows = [{
		"id": "1",
		"status": "stopped",
		"runs": 0,
		"meta": {
			"name": "Twitter Zapper",
			"author": "Don",
			"company": "Continuuity",
			"email": "mosites@gmail.com",
			"namespace": "com.continuuity.zapper"
		},
		"flowlets": [{
			"id": "1",
			"name": "Twitter",
			"className": "",
			"schema": {
				"in": {},
				"out": {}
			},
			"tuples": 0
		}, {
			"id": "2",
			"name": "Zapper",
			"className": "",
			"schema": {
				"in": {},
				"out": {}
			},
			"tuples": 0
		}, {
			"id": "3",
			"name": "Zinger",
			"className": "",
			"schema": {
				"in": {},
				"out": {}
			},
			"tuples": 0
		}, {
			"id": "4",
			"name": "Banger",
			"className": "",
			"schema": {
				"in": {},
				"out": {}
			},
			"tuples": 0
		}, {
			"id": "5",
			"name": "Disk Spinner",
			"className": "",
			"schema": {
				"in": {},
				"out": {}
			},
			"tuples": 0
		}],
		"instances": [{ "id": "1" }],
		"connections": {
			"1": [],
			"2": ["1"],
			"3": ["2"],
			"4": ["3"],
			"5": ["4"]
		}
	}];

	var history = {
		"1": []
	};

	var timeout_intervals = {};
	function timeout (flow) {
		timeout_intervals[flow.id] = setTimeout(function () {
			increment(flow);
		}, 250);
	}
	function increment (flow) {
		for (var i = 0; i < flow.flowlets.length; i ++) {
			flow.flowlets[i].tuples += Math.round(Math.ceil(Math.random() * 100) / (i + 1));
		}
		timeout(flow);
	}

	var spec = {
		"flows": function (id) {

			if (id) {
				id = id + "";
				return get_flow(id);
			} else {
				return flows;
			}

		},
		"start": function (id) {

			var flow = get_flow(id);

			flow.status = 'running';
			flow.started = toISO8601(new Date());
			flow.runs ++;
			
			history[id].push({
				act: 'start',
				result: 'success',
				time: flow.started,
				user: 'demouser'
			});

			timeout(flow);

		},
		"stop": function (id) {

			clearTimeout(timeout_intervals[id]);
			delete timeout_intervals[id];

			var flow = get_flow(id);

			flow.status = 'stopped';
			flow.stopped = toISO8601(new Date());

			history[id].push({
				act: 'stop',
				result: 'success',
				time: flow.stopped,
				user: 'demouser'
			});

			for (var i = 0; i < flow.flowlets; i ++) {
				flow.flowlets[i].tuples = 0;
			}

		},
		"history": function (id) {
			
			return history[id];

		},
		"status": function (id) {

			var flow = get_flow(id);
			var flowlets = [];

			for (var i = 0; i < flow.flowlets.length; i ++) {
				flowlets.push({
					"id": flow.flowlets[i].id,
					"status": "running",
					"tuples": {
						"consumed": flow.flowlets[i].tuples,
						"processed": flow.flowlets[i].tuples,
						"emitted": flow.flowlets[i].tuples
					}
				});
			}
			return {
				"id": id,
				"status": flow.status,
				"lastStarted": flow.started,
				"lastStopped": flow.stopped,
				"runs": flow.runs,
				"flowlets": flowlets
			};

		}
	};

	function get_flow(id) {
		for(var i = 0; i < flows.length; i ++) {
			if (flows[i].id === id) {
				return flows[i];
			}
		}
	}

	function toISO8601(date) {
		var pad_two = function(n) {
			return (n < 10 ? '0' : '') + n;
		};
		var pad_three = function(n) {
			return (n < 100 ? '0' : '') + (n < 10 ? '0' : '') + n;
		};
		return [
			date.getUTCFullYear(), '-',
			pad_two(date.getUTCMonth() + 1), '-',
			pad_two(date.getUTCDate()), 'T',
			pad_two(date.getUTCHours()), ':',
			pad_two(date.getUTCMinutes()), ':',
			pad_two(date.getUTCSeconds()), '.',
			pad_three(date.getUTCMilliseconds()), 'Z'
		].join('');
	}

	this.request = function (method, params, done) {

		done(spec[method].call(this, params));

	};

	this.subscribe = function (object, callback) {

	};

}).call(exports);
