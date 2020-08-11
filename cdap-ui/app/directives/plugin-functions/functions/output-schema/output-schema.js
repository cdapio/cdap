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
  .directive('outputSchema', function () {
    return {
      restrict: 'E',
      templateUrl: 'plugin-functions/functions/output-schema/output-schema.html',
      scope: {
        node: '=',
        fnConfig: '=',
        nodeConfig: '=',
      },
      controller: function ($scope, $uibModal, EventPipe, myPipelineApi, myHelpers) {
        var vm = this;
        var fnConfig = $scope.fnConfig;
        vm.label = fnConfig['label'] || 'Get Schema';
        vm.btnClass = fnConfig['button-class'] || 'btn-default';

        const omitProperties = myHelpers.objectQuery($scope.fnConfig, 'omit-properties') || [];
        const addProperties = myHelpers.objectQuery($scope.fnConfig, 'add-properties') || [];

        vm.node = $scope.node;
        var getRequiredFields = function() {
          if (!fnConfig['required-fields']) { return []; }
          return fnConfig['required-fields'].map( function(field) {
            if ($scope.node.plugin.properties.hasOwnProperty(field)) {
              return $scope.node.plugin.properties[field];
            }
            return '';
          });
        };
        vm.requiredProperties = getRequiredFields();
        vm.requiredFieldsWatch = $scope.$watch('OutputSchemaController.node.plugin.properties', function() {
          vm.requiredProperties = getRequiredFields();
        }, true);
        vm.missingFieldsWarningMessage = fnConfig['missing-required-fields-message'] || '';

        vm.openModal = function () {
          var modal = $uibModal.open({
            templateUrl: 'plugin-functions/functions/output-schema/output-schema-modal.html',
            windowClass: 'hydrator-modal node-config-modal layered-modal output-schema-modal',
            keyboard: true,
            controller: function ($scope, nodeInfo, $state, HydratorPlusPlusNodeService) {
              var mvm = this;
              mvm.additionalPropertiesFields = addProperties;
              mvm.additionalProperties = {};
              if(Array.isArray(mvm.additionalPropertiesFields)) {
                mvm.additionalPropertiesFields.forEach(additionalProperty => {
                  let pluginProperty = additionalProperty['plugin-property-for-value'];
                  if (pluginProperty) {
                    mvm.additionalProperties[additionalProperty.name] = nodeInfo.plugin.properties[pluginProperty];
                  }
                });
              }

              const parseResSchema = (res) => {
                if (res.name && res.type && res.fields) {
                  return [HydratorPlusPlusNodeService.getOutputSchemaObj(res)];
                }

                let schemaArr = [];
                angular.forEach(res, (value, key) => {
                  if (value.name && value.type && value.fields) {
                    schemaArr.push(HydratorPlusPlusNodeService.getOutputSchemaObj(value, key));
                  }
                });
                let recordSchemas = schemaArr.filter(schema => schema.name.substring(0, 6) === 'record');
                let schemaArrWithoutRecordSchemas = _.difference(schemaArr, recordSchemas);
                let schemaArrWithSortedRecordSchemas = schemaArrWithoutRecordSchemas.concat(_.sortBy(recordSchemas, 'name'));
                return schemaArrWithSortedRecordSchemas;
              };

              mvm.stageErrors = [];

              mvm.makeRequest = () => {
                mvm.showLoading = true;

                const params = {
                  context: $state.params.namespace
                };

                const pluginInfo = angular.copy(nodeInfo.plugin);
                pluginInfo.type = nodeInfo.type;

                omitProperties.forEach((property) => {
                  delete pluginInfo.properties[property.name];
                });

                pluginInfo.properties = angular.extend({}, pluginInfo.properties, mvm.additionalProperties);

                let schemaParseError = null;
                const requestBody = {
                  stage: {
                    name: nodeInfo.name,
                    plugin: pluginInfo,
                  },
                  inputSchemas: !nodeInfo.inputSchema ? [] : nodeInfo.inputSchema.map((input) => {
                    let schema;

                    try {
                      schema = JSON.parse(input.schema);
                    } catch (e) {
                      schemaParseError = e;
                    }

                    return {
                      stage: input.name,
                      schema
                    };
                  })
                };

                if (schemaParseError) {
                  mvm.error = schemaParseError;
                  return;
                }

                myPipelineApi.validateStage(params, requestBody)
                  .$promise
                  .then((res) => {
                    if (res.errors.length > 0) {
                      mvm.stageErrors = res.errors.map(err => err.message);
                    } else {
                      const outputSchema = myHelpers.objectQuery(res, 'spec', 'outputSchema');
                      const portSchemas = myHelpers.objectQuery(res, 'spec', 'portSchemas');

                      if (!outputSchema && !portSchemas) {
                        mvm.error = 'There is no output schema.';
                        return;
                      }

                      mvm.schemas = parseResSchema(outputSchema || portSchemas);
                    }

                    mvm.showLoading = false;
                    mvm.error = null;
                  }, (err) => {
                    mvm.showLoading = false;
                    mvm.schemas = null;
                    mvm.error = err.data;
                  });
              };

              if (addProperties.length === 0) {
                mvm.makeRequest();
              }

              mvm.apply = function () {
                mvm.schemas = mvm.schemas.map(schema => {
                  return {
                    name: schema.name,
                    schema: JSON.stringify(schema.schema)
                  };
                });
                $scope.$close({
                  schemas: mvm.schemas
                });
              };

            },
            controllerAs: 'GetSchemaModalController',
            resolve: {
              nodeInfo: function () {
                return $scope.node;
              },
            }
          });

          modal.result.then(function (obj) {
            var ee = window.CaskCommon.ee(window.CaskCommon.ee);
            ee.emit('schema.import', obj.schemas);
          });
        };

        $scope.$on('$destroy', function() {
          vm.requiredFieldsWatch();
        });
      },
      controllerAs: 'OutputSchemaController'
    };
  });
