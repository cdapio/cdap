/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceTemplatesController', function ($scope, myPipelineApi, HydratorPlusPlusPluginConfigFactory, myHelpers, mySettings, $stateParams, $state, GLOBALS, $rootScope, myAlertOnValium) {

    var vm = this;
    var oldTemplateName;

    vm.GLOBALS = GLOBALS;
    vm.pluginList = [];
    vm.isEdit = false;
    vm.isDisabled = false;
    vm.configFetched = false;
    vm.noConfig = false;
    vm.pipelineTypeOptions = [
      vm.GLOBALS.etlBatch,
      vm.GLOBALS.etlRealtime
    ];
    var batchOptions = [
      GLOBALS.pluginTypes[vm.GLOBALS.etlBatch].source,
      GLOBALS.pluginTypes[vm.GLOBALS.etlBatch].sink,
      GLOBALS.pluginTypes[vm.GLOBALS.etlBatch].transform
    ];
    var realtimeOptions = [
      GLOBALS.pluginTypes[vm.GLOBALS.etlRealtime].source,
      GLOBALS.pluginTypes[vm.GLOBALS.etlRealtime].sink,
      GLOBALS.pluginTypes[vm.GLOBALS.etlRealtime].transform
    ];

    vm.pluginDescription = '';

    var plugin;

    vm.onPipelineTypeChange = function () {
      vm.pluginList = [];
      vm.pluginVersions = [];
      vm.pluginType = null;
      vm.pluginTypeOptions = vm.templateType === vm.GLOBALS.etlBatch ? batchOptions : realtimeOptions;
    };

    vm.getPluginsList = function () {
      vm.pluginName = null;
      vm.pluginVersions = [];
      plugin = null;
      vm.pluginConfig = null;
      vm.configFetched = false;

      var prom;
      var params = {
        pipelineType: vm.templateType,
        namespace: $stateParams.nsadmin,
        version: $rootScope.cdapVersion
      };
      switch (vm.pluginType) {
        case GLOBALS.pluginTypes[vm.templateType].source:
          params.extensionType = GLOBALS.pluginTypes[vm.templateType].source;
          prom = myPipelineApi.fetchSources(params).$promise;
          break;
        case GLOBALS.pluginTypes[vm.templateType].transform:
          params.extensionType = GLOBALS.pluginTypes[vm.templateType].transform;
          prom = myPipelineApi.fetchTransforms(params).$promise;
          break;
        case GLOBALS.pluginTypes[vm.templateType].sink:
          params.extensionType = GLOBALS.pluginTypes[vm.templateType].sink;
          prom = myPipelineApi.fetchSinks(params).$promise;
          break;
      }
      prom.then(function (res) {
        vm.pluginList = _.uniq(res.map(function (p) { return p.name; }));
      });
    };

    vm.onPluginSelect = function () {
      initialize();
    };

    function initialize() {
      if (!vm.pluginName) { return; }
      vm.configFetched = false;

      var fetchApi;
      switch (vm.pluginType) {
        case GLOBALS.pluginTypes[vm.templateType].source:
          fetchApi = myPipelineApi.fetchSourceProperties;
          break;
        case GLOBALS.pluginTypes[vm.templateType].transform:
          fetchApi = myPipelineApi.fetchTransformProperties;
          break;
        case GLOBALS.pluginTypes[vm.templateType].sink:
          fetchApi = myPipelineApi.fetchSinkProperties;
          break;
      }

      plugin = {
        name: vm.pluginName
      };

      var params = {
        namespace: $stateParams.nsadmin,
        pipelineType: vm.templateType,
        extensionType: vm.pluginType,
        pluginName: vm.pluginName,
        version: $rootScope.cdapVersion
      };

      fetchApi(params).$promise
        .then(function (res) {
          vm.pluginVersions = res;

          if (vm.isEdit) {
            vm.plugin = res.filter(function (obj) {
              return angular.equals(obj.artifact, vm.pluginConfig.artifact);
            })[0];
            vm.onPluginVersionSelect();
          }
        });
    }


    vm.onPluginVersionSelect = function () {
      if (!vm.plugin) { return; }

      if (!vm.pluginConfig) {
        vm.pluginConfig = {
          _backendProperties: vm.plugin.properties,
          properties: {},
          lock: {}
        };
      } else {
        vm.pluginConfig._backendProperties = vm.plugin.properties;
      }

      var artifact = {
        name: vm.plugin.artifact.name,
        version: vm.plugin.artifact.version,
        scope: vm.plugin.artifact.scope,
        key: 'widgets.' + vm.plugin.name + '-' + vm.plugin.type
      };

      HydratorPlusPlusPluginConfigFactory.fetchWidgetJson(artifact.name, artifact.version, artifact.scope, artifact.key)
        .then(function success (res) {

          vm.configFetched = true;
          vm.noConfig = false;

          vm.groupsConfig = HydratorPlusPlusPluginConfigFactory.generateNodeConfig(vm.pluginConfig._backendProperties, res);

          angular.forEach(vm.groupsConfig.groups, function (group) {
            angular.forEach(group.fields, function (field) {
              if (field.defaultValue) {
                vm.pluginConfig.properties[field.name] = vm.pluginConfig.properties[field.name] || field.defaultValue;
              }
            });
          });

          var configOutputSchema = vm.groupsConfig.outputSchema;
          // If its an implicit schema, set the output schema to the implicit schema and inform ConfigActionFactory
          if (configOutputSchema.implicitSchema) {
            var keys = Object.keys(configOutputSchema.implicitSchema);
            var formattedSchema = [];
            angular.forEach(keys, function (key) {
              formattedSchema.push({
                name: key,
                type: configOutputSchema.implicitSchema[key]
              });
            });

            vm.pluginConfig.outputSchema = JSON.stringify({ fields: formattedSchema });
          }
        }, function error () {
          // When there is no config

          vm.noConfig = true;
          vm.configFetched = true;

        });
    };


    // On Edit Mode
    if ($stateParams.pluginTemplate) {
      vm.isEdit = true;
      vm.configFetched = false;

      mySettings.get('pluginTemplates')
        .then(function (res) {
          var template = res[$stateParams.nsadmin][$stateParams.templateType][$stateParams.pluginType][$stateParams.pluginTemplate];

          vm.templateType = template.templateType;
          vm.pluginType = template.pluginType;
          vm.pluginName = template.pluginName;

          vm.pluginConfig = {
            artifact: template.artifact,
            pluginTemplate: template.pluginTemplate,
            properties: template.properties,
            outputSchema: template.outputSchema,
            lock: template.lock
          };

          oldTemplateName = template.pluginTemplate;
          vm.pluginDescription = template.description;

          initialize();

        });
    }

    vm.save = function () {

      if (!vm.pluginConfig || !vm.pluginConfig.pluginTemplate) {
        myAlertOnValium.show({
          type: 'danger',
          title: 'Error!',
          content: GLOBALS.en.admin.templateNameMissingError
        });

        return;
      }

      var list = vm.pluginList.map(function (p) { return p.name; });
      if (list.indexOf(vm.pluginConfig.pluginTemplate) !== -1) {
        myAlertOnValium.show({
          type: 'danger',
          title: 'Error!',
          content: GLOBALS.en.admin.pluginSameNameError
        });

        return;
      }

      vm.loading = true;

      var outputPropertyName = myHelpers.objectQuery(vm.groupsConfig, 'outputSchema', 'outputSchemaProperty', '0');

      if (outputPropertyName && vm.pluginConfig._backendProperties && vm.pluginConfig._backendProperties[outputPropertyName]) {
        vm.pluginConfig.properties[outputPropertyName] = vm.pluginConfig.outputSchema;
      }

      var properties = {
        artifact: vm.plugin.artifact,
        pluginTemplate: vm.pluginConfig.pluginTemplate,
        description: vm.pluginDescription,
        properties: vm.pluginConfig.properties,
        pluginType: vm.pluginType,
        templateType: vm.templateType,
        pluginName: vm.pluginName,
        outputSchema: vm.pluginConfig.outputSchema,
        lock: vm.pluginConfig.lock
      };

      var namespace = $stateParams.nsadmin;

      mySettings.get('pluginTemplates')
        .then(function(res) {

          if (!angular.isObject(res)) {
            res = {};
          }

          var config = myHelpers.objectQuery(res, namespace, properties.templateType, properties.pluginType, properties.pluginTemplate);

          if (config && !vm.isEdit) {
            myAlertOnValium.show({
              type: 'danger',
              content: GLOBALS.en.admin.templateNameExistsError
            });
            vm.loading = false;

            return;
          }

          if (vm.isEdit && oldTemplateName !== vm.pluginConfig.pluginTemplate) {
            if (config) {
              myAlertOnValium.show({
                type: 'danger',
                content: GLOBALS.en.admin.templateNameExistsError
              });
              vm.loading = false;

              return;
            } else {
              delete res[namespace][properties.templateType][properties.pluginType][oldTemplateName];
            }
          }

          var chain = [
            namespace,
            properties.templateType,
            properties.pluginType,
            properties.pluginTemplate
          ];

          myHelpers.objectSetter(res, chain, properties);

          mySettings.set('pluginTemplates', res)
            .then(function () {
              vm.loading = false;

              $state.go('admin.namespace.detail.templateslist')
                .then(function () {
                  myAlertOnValium.show({
                    type: 'success',
                    content: 'Success saving template'
                  });
                });
            });
        });
    };

  });
