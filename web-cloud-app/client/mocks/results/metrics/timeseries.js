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

    var d = new Date();

    if (pathSamples[path]) {

      var item = pathSamples[path].shift();
      item.timestamp = d.getTime();
      pathSamples[path].push(item);

    } else {

        pathSamples[path] = $.extend(true, [], sample);

        var series = pathSamples[path];
        var i = series.length;
        while (i--) {
            series[i].timestamp = d.getTime() - (i * 1000)
        }

    }

    var series = pathSamples[path].slice(0);
    if (query.count) {
       series = series.slice(series.length - query.count, series.length);
    }

    callback(200, {
        path: path,
        result: {
            start: null,
            end: null,
            data: $.extend(true, [], series)
        },
        error: null
    });

  };

});