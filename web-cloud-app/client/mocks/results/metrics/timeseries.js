/*
 * Metrics Result Mock
 */

define([], function () {
  var data = {};
  data.sample = [
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 50
    },
    {
        "timestamp": 0,
        "value": 100
    },
    {
        "timestamp": 0,
        "value": 100
    }
  ];

  data.eventsInRate = [
    {
        'timestamp': 1372402246933,
        'value': 30000
    },
    {
        'timestamp': 1372402252281,
        'value': 35000
    },
    {
        'timestamp': 1372402259642,
        'value': 40000
    },
    {
        'timestamp': 1372402261353,
        'value': 30000
    },
    {
        'timestamp': 1372402265483,
        'value': 30000
    },
    {
        'timestamp': 1372402270620,
        'value': 35000
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 10000
    },
    {
        'timestamp': 1372402276766,
        'value': 40000
    },
    {
        'timestamp': 1372402279489,
        'value': 30000
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 55000
    },
    {
        'timestamp': 1372402288464,
        'value': 60000
    },  
    {
        'timestamp': 1372402295250,
        'value': 30000
    },
  ];

  data.eventsInCount = [
    {
        'timestamp': 1372402246933,
        'value': 35000
    },
    {
        'timestamp': 1372402252281,
        'value': 35000
    },
    {
        'timestamp': 1372402259642,
        'value': 45000
    },
    {
        'timestamp': 1372402261353,
        'value': 30000
    },
    {
        'timestamp': 1372402265483,
        'value': 10000
    },
    {
        'timestamp': 1372402270620,
        'value': 0
    },
    {
        'timestamp': 1372402272737,
        'value': 30000
    },
    {
        'timestamp': 1372402275053,
        'value': 10000
    },
    {
        'timestamp': 1372402276766,
        'value': 40000
    },
    {
        'timestamp': 1372402279489,
        'value': 100000
    },
    {
        'timestamp': 1372402281606,
        'value': 45000
    },
    {
        'timestamp': 1372402283924,
        'value': 35000
    },
    {
        'timestamp': 1372402288464,
        'value': 60000
    },  
    {
        'timestamp': 1372402295250,
        'value': 30000
    },
  ];
  return data;
  return function (path, query, callback) {

    callback(200, $.extend(true, [], sample));

  };

});