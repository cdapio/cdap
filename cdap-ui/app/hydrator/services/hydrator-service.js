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

  getNodesFromStages(stages) {
    const sanitize =  window.CaskCommon.CDAPHelpers.santizeStringForHTMLID;
    let nodes = stages.map(stage => {
      let nodeInfo = angular.extend(stage, {
        type: stage.plugin.type,
        label: stage.plugin.label,
        icon: this.DAGPlusPlusFactory.getIcon(stage.plugin.name),
        id: sanitize(stage.id) || `${sanitize(stage.name)}${this.uuid.v4()}`,
      });
      return nodeInfo;
    });
    return nodes;
  }

  getNodesMap(nodes) {
    let nodesMap = {};
    nodes.forEach(node => nodesMap[node.name] = node);
    return nodesMap;
  }

  _parseNewConfigStages(config, isStudio) {
    const sanitize =  window.CaskCommon.CDAPHelpers.santizeStringForHTMLID;
    let nodes = [];
    let connections = [];
    config.stages.forEach( node => {
      let nodeInfo = angular.extend(node, {
        type: node.plugin.type,
        label: node.plugin.label,
        icon: this.DAGPlusPlusFactory.getIcon(node.plugin.name),
        id: sanitize(node.id)
      });
      nodes.push(nodeInfo);
    });
    connections = config.connections;
    // Obtaining layout of graph with Dagre
    var graph;
    if (isStudio) {
      graph = this.DAGPlusPlusFactory.getGraphLayout(nodes, connections, 200);
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
      connections: connections,
      comments: config.comments || [],
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
      graph = this.DAGPlusPlusFactory.getGraphLayout(nodes, connections, 200);
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
      extensionType: node.type || node.plugin.type,
      pluginName: node.plugin.name,
      artifactVersion: node.plugin.artifact.version,
      artifactName: node.plugin.artifact.name,
      artifactScope: node.plugin.artifact.scope,
      limit: 1,
      order: 'DESC'
    };

    return this.myPipelineApi.fetchPluginProperties(params)
      .$promise
      .then((res = []) => {
        // Since now we have added plugin artifact information to be passed in query params
        // We don't get a list (or list of different versions of the plugin) anymore. Its always a list of 1 item.
        // Overwriting artifact as UI could have artifact ranges while importing draft.
        let lastElementIndex = res.length - 1;
        node._backendProperties = res[lastElementIndex].properties || {};
        node.description = res[lastElementIndex].description;
        node.plugin.artifact = res[lastElementIndex].artifact;
        defer.resolve(node);
        return defer.promise;
      });
  }

  formatSchema (node) {
    let schema;
    let input;
    let jsonSchema;

    jsonSchema = node.outputSchema;

    try {
      input = JSON.parse(jsonSchema);
    } catch (e) {
      input = null;
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
        name: this.GLOBALS.defaultSchemaName,
        fields: properties
      };
      // turn schema into JSON string
      let json = JSON.stringify(schema);

      return json;
    } else {
      return null;
    }
  }

  formatSchemaToAvro(schema) {
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
      } catch (e) {
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
      name: outputSchema.name || this.GLOBALS.defaultSchemaName,
      type: outputSchema.type || 'record',
      fields: outputSchema.fields || fields
    });
  }

  getPrefsRelevantToMacros(resolvedPrefs = {}, macrosMap = {}) {
    try {
      resolvedPrefs = JSON.parse(angular.toJson(resolvedPrefs));
    } catch (e) {
      console.log('ERROR: ', e);
      resolvedPrefs = {};
    }
    let relevantPrefs = {};
    for (let pref in resolvedPrefs) {
      if (macrosMap.hasOwnProperty(pref)) {
        relevantPrefs[pref] = resolvedPrefs[pref];
      }
    }
    return relevantPrefs;
  }

  isVersionInRange({supportedVersion, versionRange} = {}) {
    let flattenedVersion = versionRange;
    let isNil = (value) => _.isUndefined(value) && _.isNull(value);
    if (isNil(supportedVersion) || isNil(versionRange)) {
      return false;
    }
    if (['[', '('].indexOf(versionRange[0]) !== -1) {
      const supportedVersionInst = new window.CaskCommon.Version(supportedVersion);
      const entityVersionRangeInst = new window.CaskCommon.VersionRange(versionRange);
      if (entityVersionRangeInst.versionIsInRange(supportedVersionInst)) {
        return true;
      } else {
        return false;
      }
    }

    if (supportedVersion !== versionRange) {
      return false;
    }

    return flattenedVersion;
  }

  convertMapToKeyValuePairs(obj) {
    let keyValuePairs = [];
    keyValuePairs = Object.keys(obj).map(objKey => {
      return {
        key: objKey,
        value: obj[objKey],
        uniqueId: 'id-' + this.uuid.v4()
      };
    });
    return keyValuePairs;
  }

  convertKeyValuePairsToMap(keyValues) {
    let map = {};
    if (keyValues.pairs) {
      keyValues.pairs.forEach((currentPair) => {
        if (currentPair.key.length > 0 && currentPair.key.length > 0) {
          let key = currentPair.key;
          map[key] = currentPair.value;
        }
      });
    }
    return map;
  }

  keyValuePairsHaveMissingValues(keyValues) {
    if (keyValues.pairs) {
      return keyValues.pairs.some((keyValuePair) => {
        if (keyValuePair.notDeletable && keyValuePair.provided) { return false; }
        let emptyKeyField = (keyValuePair.key.length === 0);
        let emptyValueField = (keyValuePair.value.length === 0);
        // buttons are disabled when either the key or the value of a pair is empty, but not both
        return (emptyKeyField && !emptyValueField) || (!emptyKeyField && emptyValueField);
      });
    }
    return false;
  }

  getRuntimeArgsForDisplay(currentRuntimeArgs, macrosMap, userRuntimeArgumentsMap) {
    let runtimeArguments = {};
    let providedMacros = {};

    // holds provided macros in an object here even though we don't need the value,
    // because object hash is faster than Array.indexOf
    if (currentRuntimeArgs.pairs) {
      currentRuntimeArgs.pairs.forEach((currentPair) => {
        let key = currentPair.key;
        if (currentPair.notDeletable && currentPair.provided) {
          providedMacros[key] = currentPair.value;
        }
      });
    }
    let macros = Object.keys(macrosMap).map(macroKey => {
      let provided = false;
      if (providedMacros.hasOwnProperty(macroKey)) {
        provided = true;
      }
      return {
        key: macroKey,
        value: macrosMap[macroKey],
        uniqueId: 'id-' + this.uuid.v4(),
        notDeletable: true,
        provided
      };
    });
    let userRuntimeArguments = this.convertMapToKeyValuePairs(userRuntimeArgumentsMap);
    runtimeArguments.pairs = macros.concat(userRuntimeArguments);
    return runtimeArguments;
  }

  convertRuntimeArgsToMacros(runtimeArguments) {
    let macrosMap = {};
    let userRuntimeArgumentsMap = {};
    runtimeArguments.pairs.forEach((currentPair) => {
      let key = currentPair.key;
      if (currentPair.notDeletable) {
        macrosMap[key] = currentPair.value;
      } else {
        userRuntimeArgumentsMap[key] = currentPair.value;
      }
    });
    return {
      macrosMap,
      userRuntimeArgumentsMap
    };
  }

  getMacrosWithNonEmptyValues(macrosMap) {
    let macrosMapCopy = Object.assign({}, macrosMap);
    let {keysWithMissingValue} = this.myHelpers.objHasMissingValues(macrosMapCopy);
    keysWithMissingValue.forEach(key => {
      delete macrosMapCopy[key];
    });
    return macrosMapCopy;
  }

  runtimeArgsContainsMacros(runtimeArgs) {
    return runtimeArgs.pairs.some((currentPair) => {
      return currentPair.notDeletable;
    });
  }

  containsMacro(value) {
    if (!value) { return false; }

    let beginIndex = value.indexOf('${');
    let endIndex = value.indexOf('}');

    if (beginIndex === -1 || endIndex === -1 || beginIndex > endIndex) {
      return false;
    }

    return true;
  }
}

angular.module(`${PKG.name}.feature.hydrator`)
  .service('HydratorPlusPlusHydratorService', HydratorPlusPlusHydratorService);
