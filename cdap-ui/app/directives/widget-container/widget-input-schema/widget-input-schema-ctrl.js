/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
angular.module(PKG.name + '.commons')
  .controller('MyInputSchemaCtrl', function($scope) {
    this.doLoadNextSetOfInputSchemaRows = function() {
      this.inputSchemaRowLimit += 10;
    };
    this.inputSchemaRowLimit = 15;
    this.multipleInputs = ($scope.multipleInputs === 'true' ? true : false);
    try {
      this.inputSchemas = JSON.parse($scope.inputSchema);
    } catch(e) {
      this.inputSchemas = [];
    }
    this.inputSchemas = this.inputSchemas
      .map( function(node) {
        var schema;
        try {
          schema = JSON.parse(node.schema);
        } catch(e) {
          schema = {
            'name': 'etlSchemaBody',
            'type': 'record',
            'fields': []
          };
        }
        return {
          name: node.name,
          schema: schema
        };
      });
    this.currentIndex = 0;
    this.isInStudio = $scope.isInStudio;
  });
