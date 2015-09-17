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

angular.module(PKG.name + '.feature.adapters')
  .factory('AdapterErrorFactory', function (GLOBALS) {

    function isModelValid (nodes, connections, metadata, config) {
      var validationRules = [
        hasExactlyOneSource,
        hasAtleastOneSink,
        hasNameAndTemplateType,
        checkForRequiredField,
        checkForUnconnectedNodes
      ];
      var errors = {};
      validationRules.forEach(function(rule) {
        rule.call(this, nodes, connections, metadata, config, errors);
      });

      if (Object.keys(errors).length === 0) {
        return true;
      } else {
        return errors;
      }

    }

    function addCanvasError (error, errors) {
      if (!errors.canvas) {
        errors.canvas = [error];
      } else {
        errors.canvas.push(error);
      }
    }

    function hasAtleastOneSink(nodes, connections, metadata, config, errors) {
      var sink = [];
      var artifactType = GLOBALS.pluginTypes[metadata.template.type];

      angular.forEach(nodes, function (value, key) {
        if (value.type === artifactType.sink) {
          sink.push(key);
        }
      });
      if (sink.length === 0) {
        addCanvasError('Application should have atleast 1 sink', errors);
      }
    }

    function hasExactlyOneSource(nodes, connections, metadata, config, errors) {
      var source = [];
      var artifactType = GLOBALS.pluginTypes[metadata.template.type];

      angular.forEach(nodes, function (value, key) {
        if (value.type === artifactType.source) {
          source.push(key);
        }
      });

      if (source.length === 0) {
        addCanvasError('Application can have at most 1 source', errors);
      }
    }

    function hasNameAndTemplateType(nodes, connections, metadata, config, errors) {
      var name = metadata.name;
      if (typeof name !== 'string' || !name.length) {
        errors.name = 'Application needs to have a name';
        metadata.error = 'Enter application name';
        addCanvasError('Application needs to have a name', errors);
        return;
      }

      var pattern = /^[\w]+$/;

      if (!pattern.test(name)) {
        errors.name = 'Application name can only have alphabets, numbers, and \'_\'';
        metadata.error = 'Application name can only have alphabets, numbers, and \'_\'';
        addCanvasError('Application name can only have alphabets, numbers, and \'_\'', errors);
      }

      // Should probably add template type check here. Waiting for design.
    }

    function checkForRequiredField(nodes, connections, metadata, config, errors) {

      if(config.source.name && !isValidPlugin(config.source)) {
        errors[config.source.id] = {};
        errors[config.source.id].message = 'Source is missing required fields';
        errors[config.source.id].requiredFieldCount = config.source.requiredFieldCount;
      }

      config.sinks.forEach(function(sink) {
        if (sink.name && !isValidPlugin(sink)) {
          errors[sink.id] = {};
          errors[sink.id].message = 'Sink is missing required fields';
          errors[sink.id].requiredFieldCount = sink.requiredFieldCount;
        }
      });

      config.transforms.forEach(function(transform) {
        if (transform.name && !isValidPlugin(transform)) {
          errors[transform.id] ={};
          errors[transform.id].message = 'Transform is missing required fields';
          errors[transform.id].requiredFieldCount = transform.requiredFieldCount;
        }
      });

    }
    function isValidPlugin(plugin) {
      var i;
      var keys = Object.keys(plugin.properties);
      plugin.valid = true;
      plugin.requiredFieldCount = 0;
      if (!keys.length) {
        keys = Object.keys(plugin._backendProperties);
        for (i =0; i<keys.length; i++) {
          if (plugin._backendProperties[keys[i]] && plugin._backendProperties[keys[i]].required) {
            plugin.requiredFieldCount += 1;
            plugin.valid = false;
          }
        }
        return plugin.valid;
      }
      for (i=0; i< keys.length; i++) {
        var property = plugin.properties[keys[i]];
        if (plugin._backendProperties[keys[i]] && plugin._backendProperties[keys[i]].required && (!property || property === '')) {
          plugin.valid = false;
          plugin.requiredFieldCount += 1;
        }
      }
      return plugin.valid;
    }


    /*
      This checks for unconnected nodes and for parallel connections.
      1. It will traverse the graph starting with the source, and should end in a sink.
      2. If it does start with source && end with sink, check that all connections were traversed
    */
    function checkForUnconnectedNodes(nodes, connections, metadata, config, errors) {
      var artifactType = GLOBALS.pluginTypes[metadata.template.type];
      var nodesCopy = angular.copy(nodes);

      // at this point in the checking, I can assume that there is only 1 source
      var source,
          sinks = [],
          transforms = [];

      angular.forEach(nodes, function (value, key) {
        switch (value.type) {
          case artifactType.source:
            source = key;
            break;
          case artifactType.sink:
            sinks.push(key);
            break;
          case 'transform':
            transforms.push(key);
            break;
        }
      });

      if (!source || !sinks.length) {
        return;
      }

      var sinksConnections = [];

      var connectionHash = {};
      var branch = false;

      angular.forEach(connections, function (conn) {
        nodesCopy[conn.source].visited = true;
        nodesCopy[conn.target].visited = true;

        if (!connectionHash[conn.source] || sinks.indexOf(conn.target) !== -1) {
          if (sinks.indexOf(conn.target) !== -1) {
            sinksConnections.push(conn.source);
          } else {
            connectionHash[conn.source] = {
              target: conn.target,
              visited: false
            };
          }
        } else {
          branch = true;
        }
      });

      var sinkSourceNode;
      var sinkHasSameSource = true;
      angular.forEach(sinksConnections, function (node) {
        if (!sinkSourceNode) {
          sinkSourceNode = node;
        } else if (sinkSourceNode !== node) {
          sinkHasSameSource = false;
        }
      });

      if (!sinkHasSameSource) {
        addCanvasError('Multiple sinks have to branch from the same node', errors);
        return;
      }

      if (branch) {
        addCanvasError('Branching in this application is not supported', errors);
        return;
      }


      var unattached = [];
      angular.forEach(nodesCopy, function (value, key) {
        if (!value.visited) {
          unattached.push(key);
          errors[key] = value.name + ' ' + value.type + ' is not connected to any other node';
        }
      });

      if (unattached.length > 0) {
        addCanvasError('There are unconnected nodes in this application', errors);
        return;
      }

      var currNode = source;
      while (currNode !== sinkSourceNode) {
        if (connectionHash[currNode]) {
          if (connectionHash[currNode].visited) {
            addCanvasError('There is circular connection in this application', errors);
            return;
          }

          connectionHash[currNode].visited = true;
          currNode = connectionHash[currNode].target;
        } else {
          addCanvasError('This application connections do not end in a sink', errors);
          return;
        }
      }

      var connKeys = Object.keys(connectionHash);
      for (var i = 0; i < connKeys.length; i++) {
        if (!connectionHash[connKeys[i]].visited) {
          addCanvasError('There are parallel connections inside this application', errors);
          break;
        }
      }

    }

    return {
      isModelValid: isModelValid,
      isValidPlugin: isValidPlugin,
      hasNameAndTemplateType: hasNameAndTemplateType
    };

  });
