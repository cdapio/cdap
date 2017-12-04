/*
 * Copyright © 2017 Cask Data, Inc.
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
  .controller('MyOutputSchemaCtrl', function($scope, GLOBALS, HydratorPlusPlusNodeService, $timeout, HydratorPlusPlusHydratorService) {

    let timeout;

    const setEmptyMacro = () => {
      this.outputSchemaString = '${}';

      $timeout.cancel(timeout);
      timeout = $timeout(() => {
        let elem = document.getElementById('macro-input-schema');
        angular.element(elem)[0].focus();

        if (elem.createRange) {
          let range = elem.createRange();
          range.move('character', 2);
          range.select();
        } else {
          elem.setSelectionRange(2, 2);
        }
      });
    };

    this.formatOutputSchema = () => {
      if (!$scope.schemaAdvance) {
        if (typeof $scope.node.outputSchema === 'string') {
          $scope.node.outputSchema = [HydratorPlusPlusNodeService.getOutputSchemaObj($scope.node.outputSchema)];
        }
        this.outputSchemas = $scope.node.outputSchema
          .map((node) => {
            var schema = node.schema;
            if (typeof schema === 'string') {
              try {
                schema = JSON.parse(schema);
              } catch(e) {
                schema = {
                  'name': GLOBALS.defaultSchemaName,
                  'type': 'record',
                  'fields': []
                };
              }
            }
            return {
              name: node.name,
              schema: schema
            };
          });
      } else {
        if ($scope.node.outputSchema.length > 0 && $scope.node.outputSchema[0].schema) {
          let schema = $scope.node.outputSchema[0].schema;
          if (typeof schema !== 'string') {
            schema = JSON.stringify(schema);
          }

          if (!HydratorPlusPlusHydratorService.containsMacro(schema)) {
            setEmptyMacro();
          } else {
            this.outputSchemaString = schema;
          }
        } else {
          setEmptyMacro();
        }
      }
    };

    this.formatOutputSchema();

    $scope.$watch('schemaAdvance', () => {
      this.formatOutputSchema();
    });

    $scope.$on('$destroy', () => {
      $timeout.cancel(timeout);
    });

    this.onOutputSchemaChange = (newValue) => {
      $scope.node.outputSchema = newValue;
    };
  });
