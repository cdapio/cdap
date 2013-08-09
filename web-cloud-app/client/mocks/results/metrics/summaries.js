/*
 * Summary Metrics Result Mock
 */

define([], function () {

	var sample = {

		// Overview
		'/collect/events': {
			'rate': 0,
			'min': 0,
			'max': 2,
			'avg': 1,
			'trend': 1
		},
		'/process/busyness': {
			'rate': 0,
			'min': 0,
			'max': 2,
			'avg': 1,
			'trend': 1
		},
		'/store/bytes': {
			'rate': 0,
			'min': 0,
			'max': 2,
			'avg': 1,
			'trend': 1
		},
		'/query/events': {
			'rate': 0,
			'min': 0,
			'max': 2,
			'avg': 1,
			'trend': 1
		},

		// Per app
		'/collect/events/app1': {
			'rate': 0,
			'min': 0,
			'max': 2,
			'avg': 1,
			'trend': 1
		},
		'/process/busyness/app1': {
			'rate': 0,
			'min': 0,
			'max': 2,
			'avg': 1,
			'trend': 1
		},
		'/store/bytes/app1': {
			'rate': 0,
			'min': 0,
			'max': 2,
			'avg': 1,
			'trend': 1
		},
		'/query/events/app1': {
			'rate': 0,
			'min': 0,
			'max': 2,
			'avg': 1,
			'trend': 1
		}

	};

	return function (path, query, callback) {

		callback(200, sample[path]);

	}

});