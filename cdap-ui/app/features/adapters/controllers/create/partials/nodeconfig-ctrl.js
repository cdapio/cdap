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
  .controller('NodeConfigController', function($scope, IMPLICIT_SCHEMA, MyAppDAGService, $filter, $q, $rootScope, myAdapterApi, $state, $timeout, GLOBALS, MyNodeConfigService) {

    $scope.type = MyAppDAGService.metadata.template.type;

    $scope.data = {};
    $scope.data.isModelTouched = false;

    MyNodeConfigService.registerPluginCallback(onPluginChange);

    function onPluginChange(plugin) {
      var defer = $q.defer();
      if (!$scope.data.isModelTouched) {
        $scope.plugin = plugin;
        $scope.isValidPlugin = false;
        // falsify the ng-if in the template for one tick so that the template gets reloaded
        // there by reloading the controller.
        $timeout(function() {
          $scope.isValidPlugin = Object.keys($scope.plugin).length;
          $scope.isSource = false;
          $scope.isTransform = false;
          $scope.isSink = false;
          configurePluginInfo();
        });
        defer.resolve(true);
      } else {
        console.info('You have unsaved changes do you want to save?');
        defer.resolve(false);
      }
      return defer.promise;
    }

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
          var artifactTypeExtension = GLOBALS.pluginTypes[MyAppDAGService.metadata.template.type];
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

          if ($scope.plugin.type === artifactTypeExtension.source) {
            $scope.isSource = true;
          }

          if ($scope.plugin.type === artifactTypeExtension.sink) {
            $scope.isSink = true;
          }
          if ($scope.plugin.type === 'transform') {
            $scope.isTransform = true;
          }
        });

    }

    function fetchBackendProperties(plugin, scope) {
      var defer = $q.defer();
      var sourceType = GLOBALS.pluginTypes[MyAppDAGService.metadata.template.type].source,
          sinkType = GLOBALS.pluginTypes[MyAppDAGService.metadata.template.type].sink;

      var propertiesApiMap = {
        'transform': myAdapterApi.fetchTransformProperties
      };
      propertiesApiMap[sourceType] = myAdapterApi.fetchSourceProperties;
      propertiesApiMap[sinkType] = myAdapterApi.fetchSinkProperties;

      // This needs to pass on a scope always. Right now there is no cleanup
      // happening
      var params = {
        namespace: $state.params.namespace,
        adapterType: MyAppDAGService.metadata.template.type,
        version: $rootScope.cdapVersion,
        extensionType: plugin.type,
        pluginName: plugin.name
      };
      if (scope) {
        params.scope = scope;
      }

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
