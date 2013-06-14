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
		},
		"counterSample": [
			{
				"timestamp": 0,
				"value": 234
			}
		],
		"batchMetricCounters": {
      "bytesIn": 37.34,
      "partitions": 117,
      "mapperCompletion": 74.3,
      "mapperRecordsIn": 37.6,
      "mapperRecordsOut": 846,
      "reducerCompletion": 0,
      "reducerRecordsIn": 3.6,
      "reducerRecordsOut": 846,
      "bytesOut": 0,
      "operations": 0
    }
	};

});