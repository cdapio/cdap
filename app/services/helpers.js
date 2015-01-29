/**
 * various utility functions
 */
angular.module(PKG.name+'.services').value('myHelpers', {

  foo: function() {
    return 'bar';
  }


});

/*
  Purpose: Query a json object or an array of json objects
  Usage:
    var obj1 = [
      {
        p1: 'something',
        p2: {
          p21: 'angular',
          p22: 21,
          p23: {
            p231: 'ember'
          }
        },
        p3: 1296,
        p4: [1, 2, 3]
      },
      {
        p101: 'somethingelse'
      }
    ]
    1. query(obj1, 0, 'p1') => 'something'
    2. query(obj1, 0, 'p2', 'p22') => 21
    3. query(obj1, 0, 'p2', 'p32') => { p231: 'ember'}
    4. query(obj1, 0, 'notaproperty') => null
    5. query(obj1, 0, 'p2', 'p32', 'somethingelse') => null
    6. query(obj1, 1, 'p2', 'p32') => null
 */

angular.module(PKG.name + '.services')
  .factory('myHelpersUtil', function() {
    function query(obj) {
      if (obj === null || typeof obj === "undefined") {
          return null;
      }
      for (var i = 1; i < arguments.length; i++) {
          var param = arguments[i];
          obj = obj[param];
          if (obj === null || typeof obj === "undefined") {
              return null;
          }
      }
      return obj;
    }

    return {
      objectQuery: query
    };
  });
