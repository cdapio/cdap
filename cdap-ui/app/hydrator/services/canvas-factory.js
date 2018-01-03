/*
 * Copyright © 2015-2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distribut
 ed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

  angular.module(PKG.name + '.feature.hydrator')
  .factory('HydratorPlusPlusCanvasFactory', function(myHelpers, $q, myAlertOnValium, GLOBALS) {

    /*
      This is the inner utility function that is used once we have a source node to start our traversal.
    */
    function addConnectionsInOrder(sourceConn, finalConnections, originalConnections) {
      if (sourceConn.visited) {
        return finalConnections;
      }

      sourceConn.visited = true;
      finalConnections.push(sourceConn);
      var nextConnections = originalConnections.filter(function(conn) {
        if (sourceConn.to === conn.from) {
          return conn;
        }
      });
      if (nextConnections.length) {
        nextConnections.forEach(nextConnection => {
          return addConnectionsInOrder(nextConnection, finalConnections, originalConnections);
        });
      }
    }
    /*
      This function exists because if the user adds all tranforms and sinks but no source.
      So now technically we can show the config as list of transforms and sinks but can't traverse
      through the list of connections if we always want to start with a source.
      This is for a use case where we want to start with a transform that doesn't has any input nodes (assuming that is where source will end up).

      transform1 -> transform2 -> transform3 -- Sink1
                                             |_ Sink2
                                             |_ Sink3
    */
    function findTransformThatIsSource(originalConnections) {
      var transformAsSource = {};
      function isSource (c) {
        if (c.to === connection.from) {
          return c;
        }
      }
      for (var i = 0; i < originalConnections.length; i++) {
        var connection = originalConnections[i];
        var isSoureATarget = originalConnections.filter(isSource);
        if (!isSoureATarget.length) {
          transformAsSource = connection;
          break;
        }
      }
      return transformAsSource;
    }
    /*
      Utility that will take list of connections in any order and will order it with source -> [transforms] -> [sinks].
      This will help us to construct the config that we need to send to the backend.
      Eventually this will be removed and the backend doesn't expect the config anymore.
      All the backend requires is list of nodes and list of connections and this functionaity will be moved there.
    */
    function orderConnections(connections, appType, nodes) {
      const orderConditionConnections = (conditionNodes) => {
        angular.forEach(conditionNodes, (conditionNode) => {
          let trueConnIndex = _.findIndex(finalConnections, (conn) => conn.from === conditionNode && conn.condition === true);
          let falseConnIndex = _.findIndex(finalConnections, (conn) => conn.from === conditionNode && conn.condition === false);

          if (trueConnIndex === -1 || falseConnIndex === -1 || trueConnIndex === falseConnIndex - 1) {
            return;
          }

          let falseConn = finalConnections.splice(falseConnIndex, 1)[0];
          if (falseConnIndex < trueConnIndex) {
            finalConnections.splice(trueConnIndex, 0, falseConn);
          } else {
            finalConnections.splice(trueConnIndex + 1, 0, falseConn);
          }
        });
      };

      const orderAlertErrorConnections = (alertOrErrorNodes, isErrorNodes = true) => {
        angular.forEach(alertOrErrorNodes, (node) => {
          let lastConnToThisNodeIndex = _.findLastIndex(finalConnections, conn => conn.to === node);

          if (lastConnToThisNodeIndex === -1) {
            return;
          }

          let lastConnToThisNode = finalConnections[lastConnToThisNodeIndex];
          let lastNodeToThisNode = lastConnToThisNode.from;


          let nodesToExclude;

          if (isErrorNodes) {
            nodesToExclude = errorNodes.concat(alertNodes);
          } else {
            nodesToExclude = alertNodes;
          }

          let lastNonAlertErrorConnectionFromPrevNodeIndex = _.findLastIndex(finalConnections, conn => conn.from === lastNodeToThisNode && nodesToExclude.indexOf(conn.to) === -1);

          if (lastNonAlertErrorConnectionFromPrevNodeIndex === -1|| lastNonAlertErrorConnectionFromPrevNodeIndex === lastConnToThisNodeIndex - 1) {
            return;
          }

          finalConnections.splice(lastConnToThisNodeIndex, 1);

          if (lastConnToThisNodeIndex < lastNonAlertErrorConnectionFromPrevNodeIndex) {
            finalConnections.splice(lastNonAlertErrorConnectionFromPrevNodeIndex, 0, lastConnToThisNode);
          } else {
            finalConnections.splice(lastNonAlertErrorConnectionFromPrevNodeIndex + 1, 0, lastConnToThisNode);
          }
        });
      };

      var originalConnections = angular.copy(connections);
      if (!originalConnections.length) {
        return originalConnections;
      }
      var finalConnections = [];
      var parallelConnections = [];
      var nodesMap = {};
      var conditionNodes = [];
      var alertNodes = [];
      var errorNodes = [];
      nodes.forEach(function(n) {
        let nodeName = n.name;
        nodesMap[nodeName] = n;
        if (n.type === 'condition') {
          conditionNodes.push(nodeName);
        } else if (n.type === 'alertpublisher') {
          alertNodes.push(nodeName);
        } else if (n.type === 'errortransform') {
          errorNodes.push(nodeName);
        }
      });
      let sourceConns = connections.filter(function(conn) {
        if (nodesMap[conn.from].type === GLOBALS.pluginTypes[appType].source) {
          return conn;
        }
      });

      if (!sourceConns.length) {
        sourceConns = [findTransformThatIsSource(originalConnections)];
      }

      addConnectionsInOrder(sourceConns[0], finalConnections, originalConnections);
      if (finalConnections.length < originalConnections.length) {
        originalConnections.forEach(function(oConn) {
          var match = finalConnections.filter(fConn => fConn.from === oConn.from && fConn.to === oConn.to).length === 0;
          if (match) {
            parallelConnections.push(oConn);
          }
        });
        finalConnections = finalConnections.concat(parallelConnections);
      }

      orderConditionConnections(conditionNodes);
      orderAlertErrorConnections(errorNodes);
      orderAlertErrorConnections(alertNodes, false);

      return finalConnections.map(function (conn) { delete conn.visited; return conn; });
    }

    function pruneNonBackEndProperties(config) {
      function propertiesIterator(properties, backendProperties) {
        if (backendProperties) {
          angular.forEach(properties, function(value, key) {
            // If its a required field don't remove it.
            // This is specifically for Stream Grok pattern. If the user specifies format as "grok" in Stream we need to set this property in stream. It is not sent as list of properties from backend for that plugin.
            var isRequiredField = backendProperties[key] && backendProperties[key].required;
            var isKeyFormatSetting = key === 'format.setting.pattern';
            var isPropertyEmptyOrNull = properties[key] === '' || properties[key] === null;
            var isErrorDatasetName = !backendProperties[key] && key !== 'errorDatasetName';
            var isWorkspaceID = !backendProperties[key] && key === 'workspaceId';
            if ((isKeyFormatSetting && !isPropertyEmptyOrNull) || isWorkspaceID) {
              return;
            }
            if (isErrorDatasetName || (!isRequiredField && isPropertyEmptyOrNull)) {
              delete properties[key];
            }
          });
        }
        // FIXME: Remove this once https://issues.cask.co/browse/CDAP-3614 is fixed.
        // FIXME: This should be removed. At any point in time we need the backend properties
        // to find if a predefined app or imported config to assess if a property needs some modification.
        angular.forEach(properties, function(value, key) {
          var isPropertyNotAString = angular.isDefined(properties[key]) && angular.isString(properties[key]);
          var isPropertyEmptyOrNull = properties[key] === '' || properties[key] === null;
          if (isPropertyNotAString) {
            properties[key] = properties[key].toString();
          }
          if (isPropertyEmptyOrNull) {
            delete properties[key];
          }
        });

        return properties;
      }

      if (angular.isArray(config.stages)) {
        config.stages.forEach( node => {
          if (angular.isObject(myHelpers.objectQuery( node, 'plugin', 'properties')) &&
              Object.keys(node.plugin.properties).length > 0) {
            node.plugin.properties = propertiesIterator(node.plugin.properties, node.plugin._backendProperties);
          }
        });
      }
    }

    function pruneProperties(config) {

      pruneNonBackEndProperties(config);

      if (angular.isArray(config.stages)) {
        config.stages.forEach( node => {
          delete node.plugin._backendProperties;
        });
      }
      return config;
    }

    return {
      orderConnections: orderConnections,
      pruneProperties: pruneProperties
    };
  });
