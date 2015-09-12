/*
 * Copyright © 2015 Cask Data, Inc.
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


angular.module(PKG.name + '.feature.adapters')
  .controller('NodeConfigController', function($scope, IMPLICIT_SCHEMA, MyAppDAGService, $filter, $q, $rootScope, myAdapterApi, $state, $timeout) {
    $scope.plugin = $scope.BottomPanelController.tab.plugin;
    $scope.isValidPlugin = Object.keys($scope.plugin).length;
    $scope.type = MyAppDAGService.metadata.template.type;

    if (!$scope.plugin) {
      return;
    }
    $scope.$watch(function() {
      return $scope.BottomPanelController.tab.plugin;
    }, function() {
      if (!Object.keys($scope.BottomPanelController.tab.plugin).length) {
        return;
      }
      console.info('plugin updated');
      $scope.plugin = $scope.BottomPanelController.tab.plugin;
      $scope.isValidPlugin = false;
      $timeout(function() {
        $scope.isValidPlugin = Object.keys($scope.plugin).length;
        $scope.isSource = false;
        $scope.isTransform = false;
        $scope.isSink = false;
        configurePluginInfo();
      });
    });

    function configurePluginInfo() {
      var pluginId = $scope.plugin.id;
      var input;
      $scope.isConfigTouched = true;
      var sourceConn = $filter('filter')(MyAppDAGService.connections, { target: pluginId });
      var sourceSchema = null;
      var isStreamSource = false;

      var clfSchema = IMPLICIT_SCHEMA.clf;

      var syslogSchema = IMPLICIT_SCHEMA.syslog;

      var source;
      if (sourceConn.length) {
        source = MyAppDAGService.nodes[sourceConn[0].source];
        sourceSchema = source.outputSchema;

        if (source.name === 'Stream') {
          isStreamSource = true;
        }

        if (source.properties.format && source.properties.format === 'clf') {
          sourceSchema = clfSchema;
        } else if (source.properties.format && source.properties.format === 'syslog') {
          sourceSchema = syslogSchema;
        }

      } else {
        sourceSchema = MyAppDAGService.nodes[pluginId].properties.schema || '';
      }

      fetchBackendProperties
        .call(this, $scope.plugin, $scope)
        .then(function() {
          try {
            input = JSON.parse(sourceSchema);
          } catch (e) {
            input = null;
          }

          if (isStreamSource) {
            // Must be in this order!!
            if (!input) {
              input = {
                fields: [{ name: 'body', type: 'string' }]
              };
            }

            input.fields.unshift({
              name: 'headers',
              type: {
                type: 'map',
                keys: 'string',
                values: 'string'
              }
            });

            input.fields.unshift({
              name: 'ts',
              type: 'long'
            });
          }

          $scope.inputSchema = input ? input.fields : null;
          angular.forEach($scope.inputSchema, function (field) {
            if (angular.isArray(field.type)) {
              field.type = field.type[0];
              field.nullable = true;
            } else {
              field.nullable = false;
            }
          });

          if (!$scope.plugin.outputSchema && input) {
            $scope.plugin.outputSchema = angular.copy(JSON.stringify(input)) || null;
          }

          if ($scope.plugin._backendProperties.schema) {
            $scope.$watch('plugin.outputSchema', function () {
              if (!$scope.plugin.outputSchema) {
                if ($scope.plugin.properties && $scope.plugin.properties.schema) {
                  $scope.plugin.properties.schema = null;
                }
                return;
              }

              if (!$scope.plugin.properties) {
                $scope.plugin.properties = {};
              }
              $scope.plugin.properties.schema = $scope.plugin.outputSchema;
            });
          }

          if ($scope.plugin.type === 'source') {
            $scope.isSource = true;
          }

          if ($scope.plugin.type === 'sink') {
            $scope.isSink = true;
          }
          if ($scope.plugin.type === 'transform') {
            $scope.isTransform = true;
          }
        });

    }

    function fetchBackendProperties(plugin, scope) {
      var defer = $q.defer();

      var propertiesApiMap = {
        'source': myAdapterApi.fetchSourceProperties,
        'sink': myAdapterApi.fetchSinkProperties,
        'transform': myAdapterApi.fetchTransformProperties
      };
      // This needs to pass on a scope always. Right now there is no cleanup
      // happening
      var params = {
        adapterType: MyAppDAGService.metadata.template.type,
        version: $rootScope.cdapVersion,
        namespace: $state.params.namespace
      };
      if (scope) {
        params.scope = scope;
      }
      params[plugin.type] = plugin.name;

      return propertiesApiMap[plugin.type](params)
        .$promise
        .then(function(res) {

          var pluginProperties = (res.length? res[0].properties: {});
          if (res.length && (!plugin.description || (plugin.description && !plugin.description.length))) {
            plugin.description = res[0].description;
          }
          plugin._backendProperties = pluginProperties;
          defer.resolve(plugin);
          return defer.promise;
        });
    }

  });