/*
 * Metrics Result Mock
 */

define([], function () {

  var sample = [
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
        "value": 50
    }
  ];

  var pathSamples = {};

  return function (path, query, callback) {

    if (pathSamples[path]) {

      var item = pathSamples[path].shift();
      pathSamples[path].push(item);

    } else {

      pathSamples[path] = $.extend(true, [], sample);

    }

    callback(200, $.extend(true, [], pathSamples[path]));

  };

});