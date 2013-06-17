/*
 * Counter Metrics Result Mock
 */

define([], function () {

  var sample = {
    '/store/bytes/datasets/dataset1': 37.34,
    '/store/records/datasets/dataset1': 117,
    '/process/completion/jobs/mappers/job1': 75,
    '/process/events/jobs/mappers/job1': 100,
    '/process/bytes/jobs/mappers/job1': 30.00,
    '/process/completion/jobs/reducers/job1': 0,
    '/process/events/jobs/reducers/job1': 0,
    '/process/bytes/jobs/reducers/job1': 0,
    '/store/bytes/datasets/dataset2': 0,
    '/store/records/datasets/dataset2': 0
  };

  return function (path, query, callback) {

    callback(200, sample[path]);

  };

  /*
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
		"batchMetrics": {
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
    },
    "batchAlerts": [
      {
        "type": "critical",
        "message": "sample message goes here"
      },
      {
        "type": "warning",
        "message": "sample message goes here"
      },
      {
        "type": "critical",
        "message": "sample message goes here"
      },
      {
        "type": "debug",
        "message": "sample message goes here"
      },
      {
        "type": "danger",
        "message": "sample message goes here"
      }
    ]
	};
  */

});