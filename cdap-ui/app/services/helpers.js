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

  /**
    * Purpose: Converts a list of nodes to a list of connections
    * @param  [Array] of nodes
    * @return [Array] of connections
    * Usage: Can handle all cases, including:
        1. Fork in the middle
        2. Only a fork
        3. Fork at the beginning
        4. Fork at the end
        5. Only an Action node

        var z = [
          {
            nodeType: 'ACTION',
            program: {
              programName: "asd"
            }
          }, {
            nodeType: 'FORK',
            branches: [
              [
                [
                  {
                    nodeType: 'ACTION',
                    program: {
                      programName: "1"
                    }
                  }
                ],
                [
                  {
                    nodeType: 'ACTION',
                    program: {
                      programName: "2"
                    }
                  }
                ]
              ],
              [
                {
                  nodeType: 'ACTION',
                  program: {
                    programName: "3"
                  }
                }
              ]
            ]
          }, {
            nodeType: 'ACTION',
            program: {
              programName: "4"
            }
          }
        ];
  */

  function convert(nodes, connections) {

    for (var i=0; i+1 < nodes.length; i++) {

      if ( i === 0 && nodes[i].nodeType === 'FORK') {
        flatten(null, nodes[i], nodes[i+1], connections);
      }

      if (nodes[i].nodeType === 'ACTION' && nodes[i+1].nodeType === 'ACTION') {
        connections.push({
          sourceName: nodes[i].program.programName + nodes[i].nodeId,
          targetName: nodes[i+1].program.programName + nodes[i+1].nodeId,
          sourceType: nodes[i].nodeType
        });
      } else if (nodes[i].nodeType === 'FORK') {
        flatten(nodes[i-1], nodes[i], nodes[i+1], connections);
      }

      if ( (i+1 === nodes.length-1) && nodes[i+1].nodeType === 'FORK') {
        flatten(nodes[i], nodes[i+1], null, connections);
      }
    }
  }

  /**
    * Purpose: Flatten a source-fork-target combo to a list of connections
    * @param  [Array] of nodes
    * @param  [Array] of nodes
    * @param  [Array] of nodes
    * @return [Array] of connections

  */

  function flatten(source, fork, target, connections) {
    var branches = fork.branches,
        temp = [];

    for (var i =0; i<branches.length; i++) {
      temp = branches[i];
      if(source) {
        temp.unshift(source);
      }
      if(target) {
        temp.push(target);
      }
      convert(temp, connections);
    }
  }

  /**
    Purpose: Expand a fork and convert branched nodes to a list of connections
    * @param  [Array] of nodes
    * @return [Array] of connections

  */


  function expandForks(nodes, expandedNodes) {
    for(var i=0; i<nodes.length; i++) {
      if (nodes[i].nodeType === 'ACTION') {
        expandedNodes.push(nodes[i]);
      } else if (nodes[i].nodeType === 'FORK') {
        for (var j=0; j<nodes[i].branches.length; j++) {
          expandForks(nodes[i].branches[j], expandedNodes);
        }
      }
    }
  }

  /* ----------------------------------------------------------------------- */

  return {
    deepSet: deepSet,
    deepGet: deepGet,
    objectQuery: objectQuery,
    convert: convert,
    expandForks: expandForks
  };
});
