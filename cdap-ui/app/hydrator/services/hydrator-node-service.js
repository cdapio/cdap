/*
 * Copyright © 2016 Cask Data, Inc.
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

class HydratorPlusPlusNodeService {
  constructor($q, HydratorPlusPlusHydratorService, IMPLICIT_SCHEMA, myHelpers, GLOBALS, avsc, $state, myPipelineApi) {
    'ngInject';
    this.$q = $q;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.myHelpers = myHelpers;
    this.IMPLICIT_SCHEMA = IMPLICIT_SCHEMA;
    this.GLOBALS = GLOBALS;
    this.avsc = avsc;
    this.$state = $state;
    this.myPipelineApi = myPipelineApi;
  }
  getPluginInfo(node, appType, sourceConnections, sourceNodes, artifactVersion) {
    var promise;
    if (angular.isObject(node._backendProperties) && Object.keys(node._backendProperties).length) {
      promise = this.$q.when(node);
    } else {
      promise = this.HydratorPlusPlusHydratorService.fetchBackendProperties(node, appType, artifactVersion);
    }
    return promise.then((node) => this.configurePluginInfo(node, sourceConnections, sourceNodes));
  }
  isFieldExistsInSchema(field, schema) {
    if(angular.isObject(schema) && Array.isArray(schema.fields)) {
      return schema.fields.filter(schemaField => schemaField.name === field.name).length;
    }
    return false;
  }

  parseSchema (schema) {
    let rSchema;
    if (typeof schema === 'string') {
      if (this.HydratorPlusPlusHydratorService.containsMacro(schema)) {
        return schema;
      }
      try {
        rSchema = JSON.parse(schema);
      } catch (e) {
        rSchema = null;
      }
    } else {
      rSchema = schema;
    }
    return rSchema;
  }

  getInputSchema (sourceNode, currentNode, sourceConnections) {
    if (!sourceNode.outputSchema || typeof sourceNode.outputSchema === 'string') {
      sourceNode.outputSchema = [this.getOutputSchemaObj(sourceNode.outputSchema)];
    }
  
    let schema = sourceNode.outputSchema[0].schema;
    const defer = this.$q.defer();
  
    // If the current stage is an error collector and the previous stage is a source
    // Then call validation API to get error schema of previous node and set it as input schema
    // of the current stage.
    if (currentNode.type === 'errortransform' && (sourceNode.type === 'batchsource' || sourceNode.type === 'streamingsource')) {
      const body = {
        stage: {
          name: sourceNode.name,
          plugin: sourceNode.plugin,
        },
      };
      const params = {
        context: this.$state.params.namespace,
      };
      this.myPipelineApi.validateStage(params, body).$promise.then((res) => {
        let schema = this.myHelpers.objectQuery(res, 'spec', 'errorSchema') || this.myHelpers.objectQuery(res, 'spec', 'outputSchema');
        defer.resolve(this.parseSchema(schema));
      });
      return defer.promise;
    }
  
    // If for nodes other than source set the input schema of previous stage as input
    // schema of the current stage.
    if (currentNode.type === 'errortransform' && sourceNode.type !== 'batchsource') {
      schema =
        sourceNode.inputSchema && Array.isArray(sourceNode.inputSchema) ? sourceNode.inputSchema[0].schema : sourceNode.inputSchema;
    }
  
    // If current stage connects to a port from previous stage then cycle through connections
    // and find the stage and its output schema. That is the input schema for current stage.
    if (sourceNode.outputSchema[0].name !== this.GLOBALS.defaultSchemaName) {
      const sourcePort = (sourceConnections.find((sconn) => sconn.port) || {}).port;
      const sourceSchema = sourceNode.outputSchema.filter(
        (outputSchema) => outputSchema.name === sourcePort
      );
      schema = sourceSchema[0].schema;
    }
  
    if (Object.keys(this.IMPLICIT_SCHEMA).indexOf(sourceNode.plugin.properties.format) !== -1) {
      schema = this.IMPLICIT_SCHEMA[sourceNode.plugin.properties.format];
    }
    defer.resolve(this.parseSchema(schema));
    return defer.promise;
  }

  configurePluginInfo (node, sourceConnections, sourceNodes) {
    const defer = this.$q.defer();
    if (['action', 'source'].indexOf(this.GLOBALS.pluginConvert[node.type]) !== -1) {
      defer.resolve(node);
      return defer.promise;
    }
    const inputSchemas = [];
    const allInputSchemas = sourceNodes.map((sourceNode) => {
      return this.getInputSchema(sourceNode, node, sourceConnections).then((inputSchema) => {
        const schemaContainsMacro =
          typeof inputSchema === 'string' &&
          this.HydratorPlusPlusHydratorService.containsMacro(inputSchema);
          inputSchemas.push({
          name: sourceNode.plugin.label,
          schema: schemaContainsMacro ? inputSchema : this.HydratorPlusPlusHydratorService.formatSchemaToAvro(inputSchema),
        });
      });
    });
    this.$q.all(allInputSchemas).then(() => {
      node.inputSchema = inputSchemas;
      return defer.resolve(node);
    });
    return defer.promise;
  }

  getOutputSchemaObj(schema, schemaObjName = this.GLOBALS.defaultSchemaName) {
    return {
      name: schemaObjName,
      schema
    };
  }

  getSchemaObj(fields = [], name = this.GLOBALS.defaultSchemaName, type = 'record') {
    return {
      type,
      name,
      fields
    };
  }

  shouldPropagateSchemaToNode(targetNode) {
    if (targetNode.implicitSchema || targetNode.type === 'batchjoiner' || targetNode.type === 'splittertransform') {
      return false;
    }

    // If we encounter a macro schema, stop propagataion
    let schema = targetNode.outputSchema;
    try {
      if (Array.isArray(schema)) {
        if (!_.isEmpty(schema[0].schema)) {
          this.avsc.parse(schema[0].schema, { wrapUnions: true });
        }
      } else if (typeof schema === 'string') {
        this.avsc.parse(schema, { wrapUnions: true });
      }
    } catch (e) {
      return false;
    }

    return true;
  }

  getPluginToArtifactMap(plugins = []) {
    let typeMap = {};
    plugins.forEach( plugin => {
      typeMap[plugin.name] = typeMap[plugin.name] || [];
      typeMap[plugin.name].push(plugin);
    });
    return typeMap;
  }

  getDefaultVersionForPlugin(plugin = {}, defaultVersionMap = {}) {
    if (!Object.keys(plugin).length) {
      return {};
    }
    let defaultVersionsList = Object.keys(defaultVersionMap);
    let key = `${plugin.name}-${plugin.type}-${plugin.artifact.name}`;
    let isDefaultVersionExists = defaultVersionsList.indexOf(key) !== -1;

    let isArtifactExistsInBackend = (plugin.allArtifacts || []).filter(plug => angular.equals(plug.artifact, defaultVersionMap[key]));
    if (!isDefaultVersionExists || !isArtifactExistsInBackend.length) {
      const highestVersion = window.CaskCommon.VersionUtilities.findHighestVersion(plugin.allArtifacts.map((plugin) => plugin.artifact.version), true);
      const latestPluginVersion = plugin.allArtifacts.find((plugin) => plugin.artifact.version === highestVersion);
      return this.myHelpers.objectQuery(latestPluginVersion, 'artifact');
    }

    return angular.copy(defaultVersionMap[key]);
  }
}

angular.module(PKG.name + '.feature.hydrator')
  .service('HydratorPlusPlusNodeService', HydratorPlusPlusNodeService);
