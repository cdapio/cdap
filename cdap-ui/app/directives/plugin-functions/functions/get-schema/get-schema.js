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

angular.module(PKG.name + '.commons')
  .directive('getSchema', function () {
    return {
      restrict: 'E',
      templateUrl: 'plugin-functions/functions/get-schema/get-schema.html',
      scope: {
        node: '='
      },
      controller: function ($scope, $uibModal) {
        console.log('node', $scope.node);

        var vm = this;

        vm.openModal = function () {
          $uibModal.open({
            templateUrl: 'plugin-functions/functions/get-schema/get-schema-modal.html',
            size: 'lg',
            windowTopClass: 'hydrator-modal',
            keyboard: true,
            controller: function (nodeInfo, myPipelineApi, $state) {
              var mvm = this;

              mvm.node = nodeInfo;

              mvm.fetchSchema = function () {
                var config = angular.copy(mvm.node.plugin.properties);
                config.query = config.importQuery;
                delete config.importQuery;

                var params = {
                  namespace: $state.params.namespace,
                  artifactName: mvm.node.plugin.artifact.name,
                  version: mvm.node.plugin.artifact.version,
                  pluginType: mvm.node.type,
                  pluginName: mvm.node.plugin.name,
                  methodName: 'getSchema',
                  scope: mvm.node.plugin.artifact.scope
                };

                myPipelineApi.pluginMethod(params, config)
                  .$promise
                  .then(function (res) {
                    console.log('res', res);
                  });
              };

            },
            controllerAs: 'GetSchemaModalController',
            resolve: {
              nodeInfo: function () {
                return $scope.node;
              }
            }
          });
        };
      },
      controllerAs: 'GetSchemaController'
    };
  });