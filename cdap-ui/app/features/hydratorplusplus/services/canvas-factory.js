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
 * distributed under the License is distribut
 ed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

  angular.module(PKG.name + '.feature.hydratorplusplus')
  .factory('HydratorPlusPlusCanvasFactory', function(myHelpers, $q, myAlertOnValium, GLOBALS, $filter) {
    function extractMetadataFromDraft(data) {
      var returnConfig = {};
      returnConfig.name = myHelpers.objectQuery(data, 'name');
      returnConfig.description = myHelpers.objectQuery(data, 'description');
      var template = myHelpers.objectQuery(data, 'artifact', 'name');

      returnConfig.template = {
        type: template
      };
      if (template === GLOBALS.etlBatch) {
        returnConfig.template.schedule = {};
        returnConfig.template.schedule.cron = myHelpers.objectQuery(data.config, 'schedule') || '* * * * *';
      } else if (template === GLOBALS.etlRealtime) {
        returnConfig.template.instance = myHelpers.objectQuery(data.config, 'instance') || 1;
      }
      return returnConfig;
    }

    function parseImportedJson(configJson, type) {
      var result;
      try {
        result = JSON.parse(configJson);
      } catch(e) {
        return {
          message: 'The imported config json is incorrect. Please check the JSON content',
          error: true
        };
      }

      if (result.artifact.name !== type) {
        return {
          message: 'Template imported is for ' + result.artifact.name + '. Please switch to ' + result.artifact.name + ' creation to import.',
          error: true
        };
      }
      // We need to perform more validations on the uploaded json.
      if (
          !result.config.source ||
          !result.config.sinks ||
          !result.config.transforms
        ) {
        return {
          message: 'The structure of imported config is incorrect. To the base structure of the config please try creating a new adpater and viewing the config.',
          error: true
        };
      }
      return result;
    }

    function importPipeline(files, templateType) {
      var defer = $q.defer();
      var reader = new FileReader();
      reader.readAsText(files[0], 'UTF-8');

      reader.onload = function (evt) {
        var result = parseImportedJson(evt.target.result, templateType);
        if (result.error) {
          myAlertOnValium.show({
            type: 'danger',
            content: result.message
          });
          defer.reject(result.message);
        } else {
          defer.resolve(result);
        }
      };

      reader.onerror = function (evt) {
        defer.reject(evt);
      };
      return defer.promise;
    }

    /*
      This is the inner utility function that is used once we have a source node to start our traversal.
    */
    function addConnectionsInOrder(node, finalConnections, originalConnections) {
      if (node.visited) {
        return finalConnections;
      }

      node.visited = true;
      finalConnections.push(node);
      var nextConnection = originalConnections.filter(function(conn) {
        if (node.to === conn.from) {
          return conn;
        }
      });
      if (nextConnection.length) {
        return addConnectionsInOrder(nextConnection[0], finalConnections, originalConnections);
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
      for (var i =0; i<originalConnections.length; i++) {
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
      var originalConnections = angular.copy(connections);
      if (!originalConnections.length) {
        return originalConnections;
      }
      var finalConnections = [];
      var parallelConnections = [];
      var nodesMap = {};
      nodes.forEach(function(n) {
        nodesMap[n.name] = n;
      });
      var source = connections.filter(function(conn) {
        if (nodesMap[conn.from].type === GLOBALS.pluginTypes[appType].source) {
          return conn;
        }
      });

      if (!source.length) {
        source = [findTransformThatIsSource(originalConnections)];
        addConnectionsInOrder(source, finalConnections, originalConnections);
      }

      addConnectionsInOrder(source[0], finalConnections, originalConnections);
      if (finalConnections.length < originalConnections.length) {
        originalConnections.forEach(function(oConn) {
          if ($filter('filter')(finalConnections, oConn).length === 0) {
            parallelConnections.push(oConn);
          }
        });
        finalConnections = finalConnections.concat(parallelConnections);
      }
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
            if (isKeyFormatSetting && !isPropertyEmptyOrNull) {
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
      if (myHelpers.objectQuery(config, 'source', 'plugin', 'properties') &&
          Object.keys(config.source.plugin.properties).length > 0) {
        config.source.plugin.properties = propertiesIterator(config.source.plugin.properties, config.source.plugin._backendProperties);
      }

      config.sinks.forEach(function(sink) {
        if (myHelpers.objectQuery(sink, 'plugin', 'properties') &&
            Object.keys(sink.plugin.properties).length > 0) {
          sink.plugin.properties = propertiesIterator(sink.plugin.properties, sink.plugin._backendProperties);
        }
      });

      config.transforms.forEach(function(transform) {
        if (myHelpers.objectQuery(transform, 'plugin', 'properties') &&
            Object.keys(transform.plugin.properties).length > 0) {
          transform.plugin.properties = propertiesIterator(transform.plugin.properties, transform.plugin._backendProperties);
        }
      });
    }

    function pruneProperties(config) {

      pruneNonBackEndProperties(config);

      if (config.source.plugin && (config.source.name || config.source.plugin._backendProperties)) {
        delete config.source.plugin._backendProperties;
      }

      config.sinks.forEach(function(sink) {
        delete sink.plugin._backendProperties;
      });

      config.transforms.forEach(function(t) {
        delete t.plugin._backendProperties;
      });
      return config;
    }

    return {
      extractMetadataFromDraft: extractMetadataFromDraft,
      importPipeline: importPipeline,
      parseImportedJson: parseImportedJson,
      orderConnections: orderConnections,
      pruneProperties: pruneProperties
    };
  });
