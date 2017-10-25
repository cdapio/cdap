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
  constructor($q, HydratorPlusPlusHydratorService, IMPLICIT_SCHEMA, myHelpers, GLOBALS) {
    this.$q = $q;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.myHelpers = myHelpers;
    this.IMPLICIT_SCHEMA = IMPLICIT_SCHEMA;
    this.GLOBALS = GLOBALS;
  }
  getPluginInfo(node, appType, sourceConnections, sourceNodes, artifactVersion) {
    var promise;
    if (angular.isObject(node._backendProperties) && Object.keys(node._backendProperties).length) {
      promise = this.$q.when(node);
    } else {
      promise = this.HydratorPlusPlusHydratorService.fetchBackendProperties(node, appType, artifactVersion);
    }
    return promise.then((node) => this.configurePluginInfo(node, appType, sourceConnections, sourceNodes));
  }
  configurePluginInfo(node, appType, sourceConnections, sourceNodes) {
    const getInputSchema = (sourceNode, targetNode, sourceConnections) => {
      let schema = '';
      let isStreamSource = sourceNode.plugin.name === 'Stream';
      let inputSchema;

      const isFieldExists = (field, schema) => {
        if(angular.isObject(schema) && Array.isArray(schema.fields)) {
          return schema.fields.filter(schemaField => schemaField.name === field.name).length;
        }
        return false;
      };

      if (!sourceNode.outputSchema || typeof sourceNode.outputSchema === 'string') {
        sourceNode.outputSchema = [this.getOutputSchemaObj(sourceNode.outputSchema)];
      }

      if (sourceNode.outputSchema[0].name !== this.GLOBALS.defaultSchemaName) {
        let sourcePort = sourceConnections[0].port;
        let sourceSchema = sourceNode.outputSchema.filter(outputSchema => outputSchema.name === sourcePort);
        schema = sourceSchema[0].schema;
      } else {
        schema = sourceNode.outputSchema[0].schema;
      }

      if (targetNode.type === 'errortransform') {
        if (this.GLOBALS.pluginConvert[node.type] === 'source' || (Array.isArray(sourceNode.inputSchema) && !sourceNode.inputSchema.length)) {
          return null;
        }
        schema = sourceNode.inputSchema && Array.isArray(sourceNode.inputSchema) ? sourceNode.inputSchema[0].schema : sourceNode.inputSchema;
      }

      if (Object.keys(this.IMPLICIT_SCHEMA).indexOf(sourceNode.plugin.properties.format) !== -1) {
        schema = this.IMPLICIT_SCHEMA[sourceNode.plugin.properties.format];
      }

      if (typeof schema === 'string'){
        try {
          inputSchema = JSON.parse(schema);
        } catch(e) {
          inputSchema = null;
        }
      } else {
        inputSchema = schema;
      }

      if (isStreamSource) {
        let streamSchemaPrefix = [];
        const tsField = {
          name: 'ts',
          type: 'long'
        };
        const headersField = {
          name: 'headers',
          type: {
            type: 'map',
            keys: 'string',
            values: 'string'
          }
        };

        if (!isFieldExists(tsField, inputSchema)) {
          streamSchemaPrefix.push(tsField);
        }

        if (!isFieldExists(headersField, inputSchema)) {
          streamSchemaPrefix.push(headersField);
        }

        inputSchema = this.HydratorPlusPlusHydratorService.formatSchemaToAvro({
          fields: streamSchemaPrefix.concat(this.myHelpers.objectQuery(inputSchema, 'fields') || [])
        });
      }
      return inputSchema;
    };

    if (['action', 'source'].indexOf(this.GLOBALS.pluginConvert[node.type]) === -1) {
      node.inputSchema = sourceNodes.map(sourceNode => ({
        name: sourceNode.plugin.label,
        schema: this.HydratorPlusPlusHydratorService.formatSchemaToAvro(getInputSchema(sourceNode, node, sourceConnections))
      }));
    }

    return node;
  }

  getOutputSchemaObj(schema, schemaObjName) {
    if (!schemaObjName) {
      schemaObjName = this.GLOBALS.defaultSchemaName;
    }

    return {
      name: schemaObjName,
      schema
    };
  }
}
HydratorPlusPlusNodeService.$inject = ['$q', 'HydratorPlusPlusHydratorService', 'IMPLICIT_SCHEMA', 'myHelpers', 'GLOBALS'];

angular.module(PKG.name + '.feature.hydrator')
  .service('HydratorPlusPlusNodeService', HydratorPlusPlusNodeService);
