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

  return function (path, query, callback) {

    callback(200, $.extend(true, [], sample));

  };

});