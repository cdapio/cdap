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

class HydratorPlusPlusHydratorService {
  constructor(GLOBALS, DAGPlusPlusFactory, uuid, $state, $rootScope, myPipelineApi, $q, IMPLICIT_SCHEMA, DAGPlusPlusNodesStore, myHelpers) {
    this.GLOBALS = GLOBALS;
    this.DAGPlusPlusFactory = DAGPlusPlusFactory;
    this.uuid = uuid;
    this.$state = $state;
    this.$rootScope = $rootScope;
    this.myPipelineApi = myPipelineApi;
    this.$q = $q;
    this.IMPLICIT_SCHEMA = IMPLICIT_SCHEMA;
    this.DAGPlusPlusNodesStore = DAGPlusPlusNodesStore;
    this.myHelpers = myHelpers;
  }

  getNodesAndConnectionsFromConfig(pipeline, isStudio) {
    if (pipeline.config && pipeline.config.stages) {
      return this._parseNewConfigStages(pipeline.config, isStudio);
    } else {
      return this._parseOldConfig(pipeline, isStudio);
    }
  }

  _parseNewConfigStages(config, isStudio) {
    let nodes = [];
    let connections = [];
    config.stages.forEach( node => {
      let nodeInfo = angular.extend(node, {
        type: node.plugin.type,
        label: node.plugin.label,
        icon: this.DAGPlusPlusFactory.getIcon(node.plugin.name)
      });
      nodes.push(nodeInfo);
    });
    connections = config.connections;
    // Obtaining layout of graph with Dagre
    var graph;
    if (isStudio) {
      graph = this.DAGPlusPlusFactory.getGraphLayout(nodes, connections, 100);
    } else {
      graph = this.DAGPlusPlusFactory.getGraphLayout(nodes, connections);
    }

    angular.forEach(nodes, function (node) {
      node._uiPosition = {
        'top': graph._nodes[node.name].y + 'px' ,
        'left': graph._nodes[node.name].x + 'px'
      };
    });

    return {
      nodes: nodes,
      connections: connections
    };
  }

  _parseOldConfig(pipeline, isStudio) {
    let nodes = [];
    let connections = [];
    let config = pipeline.config;

    let artifact = this.GLOBALS.pluginTypes[pipeline.artifact.name];

    let source = angular.copy(config.source);
    let transforms = angular.copy(config.transforms || [])
      .map( node => {
        node.type = artifact.transform;
        node.label = node.label || node.name;
        node.icon = this.DAGPlusPlusFactory.getIcon(node.plugin.name);
        return node;
      });
    let sinks = angular.copy(config.sinks)
      .map( node => {
        node.type = artifact.sink;
        node.icon = this.DAGPlusPlusFactory.getIcon(node.plugin.name);
        return node;
      });

    if (Object.keys(source).length > 0) {
      source.type = artifact.source;
      source.icon = this.DAGPlusPlusFactory.getIcon(source.plugin.name);
      // replace with backend id
      nodes.push(source);
    }

    nodes = nodes.concat(transforms);
    nodes = nodes.concat(sinks);

    connections = config.connections;

    // Obtaining layout of graph with Dagre
    var graph;
    if (isStudio) {
      graph = this.DAGPlusPlusFactory.getGraphLayout(nodes, connections, 100);
    } else {
      graph = this.DAGPlusPlusFactory.getGraphLayout(nodes, connections);
    }
    angular.forEach(nodes, function (node) {
      node._uiPosition = {
        'top': graph._nodes[node.name].y + 'px' ,
        'left': graph._nodes[node.name].x + 'px'
      };
    });

    return {
      nodes: nodes,
      connections: connections
    };
  }
  fetchBackendProperties(node, appType, artifactVersion) {
    var defer = this.$q.defer();

    // This needs to pass on a scope always. Right now there is no cleanup
    // happening
    var params = {
      namespace: this.$state.params.namespace,
      pipelineType: appType,
      version: artifactVersion || this.$rootScope.cdapVersion,
      extensionType: node.type,
      pluginName: node.plugin.name
    };

    return this.myPipelineApi.fetchPluginProperties(params)
      .$promise
      .then((res = []) => {
        let nodeArtifact = node.plugin.artifact;
        if (node.plugin && !node.plugin.artifact && res.length) {
          let lastElementIndex = res.length - 1;
          node._backendProperties = res[lastElementIndex].properties || {};
          node.description = res[lastElementIndex].description;
          node.plugin.artifact = res[lastElementIndex].artifact;
          defer.resolve(node);
        } else {
          let match = res.filter(plug => angular.equals(plug.artifact, nodeArtifact));
          let pluginProperties = (match.length? match[0].properties: {});
          if (res.length && !this.myHelpers.objectQuery(node, 'description', 'length')) {
            node.description = res[0].description;
          }
          node._backendProperties = pluginProperties;
          defer.resolve(node);
        }
        return defer.promise;
      });
  }

  formatSchema (node) {

    let isStreamSource = node.name === 'Stream';
    let schema;
    let input;
    let jsonSchema;

    if (isStreamSource) {
      let availableImplicitSchema = Object.keys(this.IMPLICIT_SCHEMA);

      if (availableImplicitSchema.indexOf(node.plugin.properties.format) !== -1) {
        jsonSchema = this.IMPLICIT_SCHEMA[node.plugin.properties.format];
      } else {
        jsonSchema = node.outputSchema;
      }
    } else {
      jsonSchema = node.outputSchema;
    }

    try {
      input = JSON.parse(jsonSchema);
    } catch (e) {
      input = null;
    }

    if (isStreamSource) {
      // Must be in this order!!
      if (!input) {
        input = {
          fields: [{ name: 'body', type: 'string' }]
        };

        input.fields.unshift({
          name: 'headers',
          type: {
            type: 'map',
            keys: 'string',
            values: 'string'
          }
        });

        input.fields.unshift({
          name: 'ts',
          type: 'long'
        });
      }
    }

    schema = input ? input.fields : null;
    angular.forEach(schema, function (field) {
      if (angular.isArray(field.type)) {
        field.type = field.type[0];
        field.nullable = true;
      } else {
        field.nullable = false;
      }
    });

    return schema;

  }

  generateSchemaOnEdge(sourceId) {
    var nodes = this.DAGPlusPlusNodesStore.getNodes();
    var sourceNode;

    for (var i = 0; i<nodes.length; i++) {
      if (nodes[i].name === sourceId) {
        sourceNode = nodes[i];
        break;
      }
    }

    return this.formatSchema(sourceNode);
  }

  formatOutputSchema (schemaArray) {
    let typeMap = 'map<string, string>';
    let mapObj = {
      type: 'map',
      keys: 'string',
      values: 'string'
    };

    let properties = [];
    angular.forEach(schemaArray, function(p) {
      if (p.name) {
        var property;
        if (p.type === typeMap) {
          property = angular.copy(mapObj);
        } else {
          property = p.type;
        }

        properties.push({
          name: p.name,
          type: p.nullable ? [property, 'null'] : property
        });
      }
    });

    // do not include properties on the request when schema field is empty
    if (properties.length !== 0) {
      let schema = {
        type: 'record',
        name: 'etlSchemaBody',
        fields: properties
      };
      // turn schema into JSON string
      let json = JSON.stringify(schema);

      return json;
    } else {
      return null;
    }
  }
  formatOutputSchemaToAvro(schema) {
    let typeMap = 'map<string, string>';
    let mapObj = {
      type: 'map',
      keys: 'string',
      values: 'string'
    };
    let fields = [];
    let outputSchema;
    if (typeof schema === 'string') {
      try {
        outputSchema = JSON.parse(schema);
      } catch(e) {
        console.log('ERROR: Parsing schema JSON ', e);
        return schema;
      }
    } else if (schema === null || typeof schema === 'undefined' ) {
      return '';
    } else {
      outputSchema = angular.copy(schema);
    }
    if (outputSchema.name && outputSchema.type && outputSchema.fields) {
      return JSON.stringify(outputSchema);
    }
    fields = Object.keys(outputSchema).map(field => {
      if (outputSchema[field] === typeMap) {
        return {
          name: field,
          type: mapObj
        };
      }
      return {
        name: field,
        type: outputSchema[field]
      };
    });
    return JSON.stringify({
      name: outputSchema.name || 'etlSchemaBody',
      type: outputSchema.type || 'record',
      fields: outputSchema.fields || fields
    });
  }
}
HydratorPlusPlusHydratorService.$inject = ['GLOBALS', 'DAGPlusPlusFactory', 'uuid', '$state', '$rootScope', 'myPipelineApi', '$q', 'IMPLICIT_SCHEMA', 'DAGPlusPlusNodesStore', 'myHelpers'];
angular.module(`${PKG.name}.feature.hydrator`)
  .service('HydratorPlusPlusHydratorService', HydratorPlusPlusHydratorService);
