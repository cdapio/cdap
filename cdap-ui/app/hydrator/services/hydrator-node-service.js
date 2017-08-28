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
  getPluginInfo(node, appType, sourceConn, artifactVersion) {
    var promise;
    if (angular.isObject(node._backendProperties) && Object.keys(node._backendProperties).length) {
      promise = this.$q.when(node);
    } else {
      promise = this.HydratorPlusPlusHydratorService.fetchBackendProperties(node, appType, artifactVersion);
    }
    return promise.then((node) => this.configurePluginInfo(node, appType, sourceConn));
  }
  configurePluginInfo(node, appType, sourceConn) {
    const getSchema = (node, targetNode) => {
      var schema = node.outputSchema;

      if (targetNode.type === 'errortransform' && this.GLOBALS.pluginConvert[node.type] !== 'source') {
        if (Array.isArray(node.inputSchema) && !node.inputSchema.length) {
          return null;
        }
        schema = node.inputSchema && Array.isArray(node.inputSchema) ? node.inputSchema[0].schema : node.inputSchema;
      } else if (targetNode.type === 'errortransform' && this.GLOBALS.pluginConvert[node.type] === 'source') {
        return null;
      }

      var isStreamSource = false;
      var inputSchema;
      const isFieldExists = (field, schema) => {
        if(angular.isObject(schema) && Array.isArray(schema.fields)) {
          return schema.fields.filter(schemaField => schemaField.name === field.name).length;
        }
        return false;
      };
      if (node.plugin.name === 'Stream') {
        isStreamSource = true;
      }
      if (Object.keys(this.IMPLICIT_SCHEMA).indexOf(node.plugin.properties.format) !== -1) {
        schema = this.IMPLICIT_SCHEMA[node.plugin.properties.format];
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
        let tsField = {
          name: 'ts',
          type: 'long'
        };
        let headersField = {
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

        inputSchema = this.HydratorPlusPlusHydratorService.formatOutputSchemaToAvro({
          fields: streamSchemaPrefix.concat(this.myHelpers.objectQuery(inputSchema, 'fields') || [])
        });
      }
      return inputSchema;
    };

    if (['action', 'source'].indexOf(this.GLOBALS.pluginConvert[node.type]) === -1) {
      node.inputSchema = sourceConn.map( source => ({
        name: source.plugin.label,
        schema: this.HydratorPlusPlusHydratorService.formatOutputSchemaToAvro(getSchema(source, node))
      }));
    }

    return node;
  }
}
HydratorPlusPlusNodeService.$inject = ['$q', 'HydratorPlusPlusHydratorService', 'IMPLICIT_SCHEMA', 'myHelpers', 'GLOBALS'];

angular.module(PKG.name + '.feature.hydrator')
  .service('HydratorPlusPlusNodeService', HydratorPlusPlusNodeService);
