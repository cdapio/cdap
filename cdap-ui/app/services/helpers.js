/**
 * various utility functions
 */
angular.module(PKG.name+'.services')
  .factory('myHelpers', function(){

   /**
    * set a property deep in an object
    * adapted from Y.namespace
    * http://yuilibrary.com/yui/docs/api/files/yui_js_yui.js.html#l1370
    * @param  {Object} obj object on which to set a value
    * @param  {String} key potentially nested jsonpath, eg "foo.bar.baz"
    * @param  {Mixed} val value to set at the key
    * @return {Object}     modified obj
    */
  function deepSet(obj, key, val) {
    var it = obj, j, d, m;

    if (key.indexOf('.') > -1) {
      d = key.split('.');
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

  /* ----------------------------------------------------------------------- */

  /**
   * get to a property deep in an obj by jsonpath
   * @param  {Object} obj object to inspect
   * @param  {String} key jsonpath eg "foo.bar.baz"
   * @return {Mixed}     value at the
   */
  function deepGet(obj, key) {
    return objectQuery.apply(null, [obj].concat(key.split('.')));
  }

  /* ----------------------------------------------------------------------- */

  /*
    Purpose: Query a json object or an array of json objects
    Return: Returns undefined if property is not defined(never set) and
            and a valid value (including null) if defined.
    Usage:
      var obj1 = [
        {
          p1: 'something',
          p2: {
            p21: 'angular',
            p22: 21,
            p23: {
              p231: 'ember',
              p232: null
            }
          },
          p3: 1296,
          p4: [1, 2, 3],
          p5: null
        },
        {
          p101: 'somethingelse'
        }
      ]
      1. query(obj1, 0, 'p1') => 'something'
      2. query(obj1, 0, 'p2', 'p22') => 21
      3. query(obj1, 0, 'p2', 'p32') => { p231: 'ember'}
      4. query(obj1, 0, 'notaproperty') => undefined
      5. query(obj1, 0, 'p2', 'p32', 'somethingelse') => undefined
      6. query(obj1, 1, 'p2', 'p32') => undefined
      7. query(obj1, 0, 'p2', 'p23', 'p232') => null
      8. query(obj1, 0, 'p5') => null
   */

  function objectQuery(obj) {
    if (!angular.isObject(obj)) {
        return null;
    }
    for (var i = 1; i < arguments.length; i++) {
        obj = obj[arguments[i]];
        if (!angular.isObject(obj)) {
          return obj;
        }
    }
    return obj;
  }

  /* ----------------------------------------------------------------------- */

  var roundUpToNearest = function(val, nearest) {
    return Math.ceil(val / nearest) * nearest;
  };
  var roundDownToNearest = function(val, nearest) {
    return Math.floor(val / nearest) * nearest;
  };

  function aggregate(inputMetrics, by) {
    // Given an object in the format: { ts1: value, ts2: value, ts3: value, ts4: value },
    // This will return an object in the same format, where each sequence of {by} timestamps will be summed up.
    // Not currently considering resolution of the metric values (It groups simply starting from the first timestamp),
    // as opposed to grouping into 5-minute interval.
    var aggregated = {};
    var timeValues = Object.keys(inputMetrics);
    var roundedDown = roundDownToNearest(timeValues.length, by);
    for (var i = 0; i < roundedDown; i += by) {
      var sum = 0;
      for (var j = 0; j < by; j++) {
        sum += inputMetrics[timeValues[i + j]];
      }
      aggregated[timeValues[i]] = sum;
    }
    // Add up remainder elements (in case number of elements in obj is not evenly divisible by {by}
    if (roundedDown < timeValues.length) {
      var finalKey = timeValues[roundedDown];
      aggregated[finalKey] = 0;
      for (var i = roundedDown; i < timeValues.length; i++) {
        aggregated[finalKey] += inputMetrics[timeValues[i]];
      }
    }
    return aggregated;
  }


  // 'ns.default.app.foo' -> {'ns': 'default', 'app': 'foo'}
  function contextToTags(context) {
    var parts, tags, i;
    if (context.length) {
      parts = context.split('.');
    } else {
      // For an empty context, we want no tags. Splitting it by '.' yields [""]
      parts = [];
    }
    if (parts.length % 2 !== 0) {
      throw "Metrics context has uneven number of parts: " + context;
    }
    tags = {};
    for (i = 0; i < parts.length; i+=2) {
      // In context, '~' is used to represent '.'
      var tagValue = parts[i + 1].replace(/~/g, '.');
      tags[parts[i]] = tagValue;
    }
    return tags;
  }

  // {name: k1, value: v1} -> 'k1.v2'
  function tagToContext(tag) {
    var key = tag.name.replace(/\./g, '~');
    var value = tag.value.replace(/\./g, '~');
    return key + '.' + value;
  }

  // { namespace: default, app: foo, flow: bar } -> 'tag=namespace:default&tag=app:foo&tag=flow:bar'
  function tagsToParams(tags) {
    var keys = Object.keys(tags);
    var queryParams = [];
    keys.forEach(function(key) {
      var value = tags[key];
      queryParams.push('tag=' + key + ':' + value);
    });
    return queryParams.join('&');
  }

  return {
    deepSet: deepSet,
    deepGet: deepGet,
    objectQuery: objectQuery,
    roundUpToNearest: roundUpToNearest,
    roundDownToNearest: roundDownToNearest,
    aggregate: aggregate,
    contextToTags: contextToTags,
    tagsToParams: tagsToParams,
    tagToContext: tagToContext
  };
});
