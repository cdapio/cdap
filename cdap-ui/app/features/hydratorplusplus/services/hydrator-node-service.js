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
  constructor($q, HydratorPlusPlusHydratorService, IMPLICIT_SCHEMA) {
    this.$q = $q;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
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
      var input;
      var sourceSchema = null;
      var isStreamSource = false;

      var source;
      if (sourceConn && sourceConn.length) {
        source = sourceConn[0];
        sourceSchema = source.outputSchema;

        if (source.plugin.name === 'Stream') {
          isStreamSource = true;
        }

        if (Object.keys(this.IMPLICIT_SCHEMA).indexOf(source.plugin.properties.format) !== -1) {
          sourceSchema = this.IMPLICIT_SCHEMA[source.plugin.properties.format];
        }

      } else {
        sourceSchema = '';
      }

      try {
        input = JSON.parse(sourceSchema);
      } catch (e) {
        input = null;
      }

      node.inputSchema = input ? input.fields : null;
      angular.forEach(node.inputSchema, function (field) {
        if (angular.isArray(field.type)) {
          field.type = field.type[0];
          field.nullable = true;
        } else {
          field.nullable = false;
        }
        if (isStreamSource) {
          delete field.readonly;
        }
      });
      return node;
    }
}
HydratorPlusPlusNodeService.$inject = ['$q', 'HydratorPlusPlusHydratorService', 'IMPLICIT_SCHEMA'];

angular.module(PKG.name + '.feature.hydratorplusplus')
  .service('HydratorPlusPlusNodeService', HydratorPlusPlusNodeService);
