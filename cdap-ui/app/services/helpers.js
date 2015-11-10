/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

/**
 * various utility functions
 */
angular.module(PKG.name+'.services')
  .factory('myHelpers', function(myCdapUrl){

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
   * @param  {Boolean} returns a copy when isCopy === true
   * @return {Mixed}     value at the
   */
  function deepGet(obj, key, isCopy) {
    var val = objectQuery.apply(null, [obj].concat(key.split('.')));
    return isCopy ? angular.copy(val) : val;
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

  function __generateConfig(isNsPath, method, type, path, isArray, customConfig) {
    var config = {
      method: method,
      options: { type: type}
    };
    if (isNsPath) {
      config.url = myCdapUrl.constructUrl({ _cdapNsPath: path });
    } else {
      config.url = myCdapUrl.constructUrl({ _cdapPath: path });
    }
    if (isArray) {
      config.isArray = true;
    }

    return angular.extend(config, customConfig || {});
  }

  /*
    Purpose: construct a resource config object for endpoints API services
  */

  function getConfigNs (method, type, path, isArray, customConfig) {
    return __generateConfig(true, method, type, path, isArray, customConfig);
  }

  function getConfig (method, type, path, isArray, customConfig) {
    return __generateConfig(false, method, type, path, isArray, customConfig);
  }

  /* ----------------------------------------------------------------------- */

  return {
    deepSet: deepSet,
    deepGet: deepGet,
    objectQuery: objectQuery,
    getConfig: getConfig,
    getConfigNs: getConfigNs
  };
});
