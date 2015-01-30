/**
 * various utility functions
 */

var PERIOD = '.';

angular.module(PKG.name+'.services')
  .factory('myHelpers', function(){


   /**
    * set a property deep in an object
    * adapted from Y.namespace
    * http://yuilibrary.com/yui/docs/api/files/yui_js_yui.js.html#l1370
    * @param  {Object} obj object on which to set a value
    * @param  {String} key potentially nested key, eg "foo.bar.baz"
    * @param  {Mixed} val value to set at the key
    * @return {Object}     modified obj
    */
  function deepSet(obj, key, val) {
    var it = obj, j, d, m;

    if (key.indexOf(PERIOD) > -1) {
      d = key.split(PERIOD);
      m = d.length-1;
      for (j = 0; j <= m; j++) {
        if(j!==m) {
          it[d[j]] = it[d[j]] || {};
          it = it[d[j]];
        }
        else { // last part
          it[d[m]] = val;
        }
      }
    } else {
      obj[key] = val;
    }

    return obj;
  }








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

  function objectQuery(obj) {
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


  /* ----------------------------------------------------------------------- */

  return {
    deepSet: deepSet,
    objectQuery: objectQuery
  }
});
