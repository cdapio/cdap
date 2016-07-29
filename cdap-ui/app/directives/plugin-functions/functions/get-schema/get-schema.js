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
        node: '=',
        fnConfig: '='
      },
      controller: function ($scope, $uibModal, EventPipe, myPipelineApi) {
        var vm = this;
        var fnConfig = $scope.fnConfig;
        var methodName = fnConfig['plugin-method'] || 'getSchema';
        var methodType = fnConfig.method || 'GET';
        var getPluginMethodApi = function(methodType) {
          switch(methodType) {
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

        vm.openModal = function () {
          var modal = $uibModal.open({
            templateUrl: 'plugin-functions/functions/get-schema/get-schema-modal.html',
            windowClass: 'hydrator-modal node-config-modal',
            keyboard: true,
            controller: function ($scope, nodeInfo, $state) {
              var mvm = this;

              mvm.node = angular.copy(nodeInfo);

              mvm.query = mvm.node.plugin.properties.importQuery;

              mvm.fetchSchema = function () {
                var config = mvm.node.plugin.properties;
                config.query = mvm.query;
                delete config.importQuery;

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
                    mvm.resolvedSchema = angular.copy(res);
                    mvm.schema = res.fields.filter(function (field) {
                      if (angular.isArray(field.type)) {
                        field.type = field.type[0];
                        field.nullable = true;
                      }
                      return field;
                    });
                  }, function (err) {
                    mvm.resolvedSchema = null;
                    mvm.schema = null;

                    mvm.error = err.data;
                  });
              };

              mvm.apply = function () {
                $scope.$close({
                  schema: mvm.resolvedSchema,
                  query: mvm.query
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

          modal.result.then(function (obj) {
            EventPipe.emit('schema.import', JSON.stringify(obj.schema));
            $scope.node.plugin.properties.importQuery = obj.query;
          });
        };
      },
      controllerAs: 'GetSchemaController'
    };
  });
