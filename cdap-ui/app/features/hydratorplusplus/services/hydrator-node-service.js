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
  constructor($q, HydratorPlusPlusHydratorService, IMPLICIT_SCHEMA, myHelpers) {
    this.$q = $q;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.myHelpers = myHelpers;
    this.IMPLICIT_SCHEMA = IMPLICIT_SCHEMA;
  }
  getPluginInfo(node, appType, sourceConn) {
    var promise;
    if (node._backendProperties) {
      promise = this.$q.when(node);
    } else {
      promise = this.HydratorPlusPlusHydratorService.fetchBackendProperties(node, appType);
    }
    return promise.then((node) => this.configurePluginInfo(node, appType, sourceConn));
  }
  configurePluginInfo(node, appType, sourceConn) {
    const getSchema = (node) => {
      var schema = node.outputSchema;
      var isStreamSource = false;
      var inputSchema;
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
        let streamSchemaPrefix = [
          {
            name: 'ts',
            type: 'long'
          },
          {
            name: 'headers',
            type: {
              type: 'map',
              keys: 'string',
              values: 'string'
            }
          }
        ];

        inputSchema = this.HydratorPlusPlusHydratorService.formatOutputSchemaToAvro({
          fields: streamSchemaPrefix.concat(this.myHelpers.objectQuery(inputSchema, 'fields') || [])
        });
      }
      return inputSchema;
    };

    node.inputSchema = sourceConn.map( source => ({
      name: source.plugin.label,
      schema: this.HydratorPlusPlusHydratorService.formatOutputSchemaToAvro(getSchema(source))
    }));

    return node;
  }
}
HydratorPlusPlusNodeService.$inject = ['$q', 'HydratorPlusPlusHydratorService', 'IMPLICIT_SCHEMA', 'myHelpers'];

angular.module(PKG.name + '.feature.hydratorplusplus')
  .service('HydratorPlusPlusNodeService', HydratorPlusPlusNodeService);
