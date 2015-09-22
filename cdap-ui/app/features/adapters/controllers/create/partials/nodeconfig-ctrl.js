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
  .controller('NodeConfigController', function($scope, IMPLICIT_SCHEMA, MyAppDAGService, $filter, $q, $rootScope, myAdapterApi, $state, $timeout, GLOBALS, MyNodeConfigService, $bootstrapModal) {

    $scope.type = MyAppDAGService.metadata.template.type;

    $scope.data = {};

    MyNodeConfigService.registerPluginSetCallback($scope.$id, onPluginChange);
    MyNodeConfigService.registerRemovePluginCallback($scope.$id, onPluginRemoved);

    function onPluginRemoved(nodeId) {
      if ($scope.plugin && $scope.plugin.id === nodeId){
        $scope.isValidPlugin = false;
        MyNodeConfigService.setIsPluginBeingEdited(false);
      }
    }

    function onPluginChange(plugin) {
      var defer = $q.defer();
      $scope.type = MyAppDAGService.metadata.template.type;
      if (plugin && $scope.plugin && plugin.id === $scope.plugin.id) {
        return;
      }
      if (!MyNodeConfigService.getIsPluginBeingEdited()) {
        switchPlugin(plugin);
        defer.resolve(true);
      } else {
        confirmPluginSwitch()
          .then(
            function yes() {
              switchPlugin(plugin);
            },
            function no() {
              MyNodeConfigService.resetPlugin($scope.plugin);
              console.log('User chose to stay in the same plugin');
            }
          );
        defer.resolve(false);
      }
      return defer.promise;
    }

    function confirmPluginSwitch() {
      var defer = $q.defer();

      $bootstrapModal.open({
        keyboard: false,
        templateUrl: '/assets/features/adapters/templates/partial/confirm.html',
        windowClass: 'modal-confirm',
        controller: ['$scope', function ($scope) {
          $scope.continue = function () {
            $scope.$close('close');
          };

          $scope.cancel = function () {
            $scope.$close('keep open');
          };
        }]
      }).result.then(function (closing) {
        if (closing === 'close') {
          defer.resolve(true);
        } else {
          defer.reject(false);
        }
      });
      return defer.promise;
    }

    function switchPlugin(plugin) {
      $scope.plugin = plugin;
      $scope.isValidPlugin = false;
      // falsify the ng-if in the template for one tick so that the template gets reloaded
      // there by reloading the controller.
      $timeout(setPluginInfo);
    }

    function setPluginInfo() {
      $scope.isSource = false;
      $scope.isTransform = false;
      $scope.isSink = false;
      configurePluginInfo().then(
        function success() {
          $scope.isValidPlugin = Object.keys($scope.plugin).length;
        },
        function error() {
          console.error('Fetching backend properties for :',$scope.plugin.name, ' failed.');
        });
    }

    function configurePluginInfo() {
      var defer = $q.defer();

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
        .then(
        function success() {
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
            $scope.plugin.outputSchema = JSON.stringify(input) || null;
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
          defer.resolve(true);
        },
        function error() {
          defer.reject(false);
        }
      );

      return defer.promise;
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

    $scope.$on('$destroy', function() {
      MyNodeConfigService.unRegisterPluginSetCallback($scope.$id);
      MyNodeConfigService.unRegisterRemovePluginCallback($scope.$id);
    });

  });
