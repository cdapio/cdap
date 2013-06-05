/*
 * Counter Metrics Result Mock
 */

define([], function () {

	return {
		"/counters/flow?a,b,c,d": {
			"status": 200,
			"result": {
				"values": {
					"a": 1,
					"b": 1,
					"c": 1,
					"d": 1
				}
			}
		}
	};

});