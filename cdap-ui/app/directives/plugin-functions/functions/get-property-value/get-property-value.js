/*
 * Copyright © 2018 Cask Data, Inc.
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
  .directive('getPropertyValue', function() {
    return {
      restrict: 'E',
      templateUrl: 'plugin-functions/functions/get-property-value/get-property-value.html',
      scope: {
        node: '=',
        fnConfig: '=',
        nodeConfig: '='
      },
      controller: function ($scope, $uibModal, myPipelineApi, myHelpers) {
        var vm = this;
        var fnConfig = $scope.fnConfig;
        var methodName = fnConfig['plugin-method'] || 'getSchema';
        vm.label = myHelpers.objectQuery(fnConfig, 'widget-attributes', 'label') || 'Get value';
        vm.node = $scope.node;
        var getRequiredFields = function () {
          if (!fnConfig['required-fields']) { return []; }
          return fnConfig['required-fields'].map(function (field) {
            if ($scope.node.plugin.properties.hasOwnProperty(field)) {
              return $scope.node.plugin.properties[field];
            }
            return '';
          });
        };
        vm.requiredProperties = getRequiredFields();
        vm.requiredFieldsWatch = $scope.$watch('GetPropertyValueController.node.plugin.properties', function () {
          vm.requiredProperties = getRequiredFields();
        }, true);
        vm.missingFieldsWarningMessage = fnConfig['missing-required-fields-message'] || '';
        var methodType = fnConfig.method || 'GET';
        var getPluginMethodApi = function (methodType) {
          switch (methodType) {
            case 'POST':
              return myPipelineApi.postPluginMethod;
            case 'GET':
              return myPipelineApi.getPluginMethod;
            case 'PUT':
              return myPipelineApi.putPluginMethod;
            case 'DELETE':
              return myPipelineApi.deletePluginMethod;
          }
        };
        var pluginMethodApi = getPluginMethodApi(methodType);

        // @ts-ignore
        vm.openModal = function () {
          var modal = $uibModal.open({
            templateUrl: 'plugin-functions/functions/get-property-value/get-property-value-modal.html',
            windowClass: 'hydrator-modal layered-modal get-property-value-modal',
            keyboard: true,
            controller: function ($scope, nodeInfo, $state) {
              var mvm = this;
              mvm.showLoading = true;
              mvm.node = angular.copy(nodeInfo);
              mvm.fetchValue = function () {
                var config = mvm.node.plugin.properties;
                var params = {
                  namespace: $state.params.namespace,
                  artifactName: mvm.node.plugin.artifact.name,
                  version: mvm.node.plugin.artifact.version,
                  pluginType: mvm.node.type,
                  pluginName: mvm.node.plugin.name,
                  methodName: methodName,
                  scope: mvm.node.plugin.artifact.scope
                };


                pluginMethodApi(params, config)
                  .$promise
                  .then(function (res) {
                    mvm.error = null;
                    mvm.propertyValue = res;
                    mvm.showLoading = false;
                  }, function (err) {
                    mvm.propertyValue = null;
                    mvm.error = err.data;
                    mvm.showLoading = false;
                  });
              };

              mvm.apply = function () {
                $scope.$close(JSON.stringify(mvm.propertyValue));
              };

              mvm.fetchValue();

            },
            controllerAs: 'GetPropertyValueModalController',
            resolve: {
              nodeInfo: function () {
                return $scope.node;
              }
            }
          });

          modal.result.then(function (value) {
            let outputProperty = $scope.nodeConfig.name;
            $scope.node.plugin.properties[outputProperty] = value;
          });
        };
      },
      controllerAs: 'GetPropertyValueController'
    };
  });
