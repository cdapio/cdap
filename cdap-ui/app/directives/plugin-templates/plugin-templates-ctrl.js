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

angular.module(`${PKG.name}.commons`)
  .controller('PluginTemplatesCtrl', function (PluginTemplatesDirStore, PluginTemplatesDirActions, $scope, myPipelineApi, HydratorPlusPlusPluginConfigFactory, myHelpers, mySettings, $stateParams, $state, GLOBALS, $rootScope, HydratorPlusPlusNodeService, HydratorPlusPlusHydratorService) {

    var vm = this;
    var oldTemplateName;
    vm.prefill = {};
    vm.GLOBALS = GLOBALS;
    vm.pluginList = [];
    vm.isEdit = $scope.mode === 'edit'? true: false;
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
      vm.submitted = false;
      vm.pluginList = [];
      vm.pluginVersions = [];
      vm.pluginType = null;
      vm.pluginTypeOptions = vm.templateType === vm.GLOBALS.etlBatch ? batchOptions : realtimeOptions;
    };

    vm.getPluginsList = function (pluginType) {
      vm.submitted = false;
      vm.pluginName = PluginTemplatesDirStore.getPluginName();
      vm.pluginVersions = [];
      plugin = null;
      vm.pluginConfig = null;
      vm.configFetched = false;

      var prom;
      var params = {
        pipelineType: vm.templateType,
        namespace: $stateParams.namespace,
        version: $rootScope.cdapVersion,
        extensionType: pluginType
      };
      prom = myPipelineApi.fetchPlugins(params).$promise;
      prom.then(function (res) {
        vm.pluginList = _.uniq(res.map(function (p) { return p.name; }));
      });
    };

    vm.onPluginSelect = function () {
      vm.submitted = false;
      initialize();
    };

    function initialize() {
      if (!vm.pluginName) { return; }
      vm.configFetched = false;

      var fetchApi = myPipelineApi.fetchPluginProperties;

      var params = {
        namespace: $stateParams.namespace,
        pipelineType: vm.templateType,
        extensionType: vm.pluginType,
        pluginName: vm.pluginName,
        version: $rootScope.cdapVersion
      };

      fetchApi(params).$promise
        .then(function (res) {
          vm.pluginVersions = res;
          let latestArtifact, baseVersion = new window.CaskCommon.Version('0.0.0');
          let availableArtifacts = _.map(res, 'artifact');

          angular.forEach(availableArtifacts, function (artifactObj) {
            let availableversion = new window.CaskCommon.Version(artifactObj.version);
            let compare = availableversion.compareTo(baseVersion);

              if (compare > 0) {
              latestArtifact = artifactObj;
              baseVersion = availableversion;
            }
          });

          vm.plugin = res.filter(function (obj) {
            if (!vm.pluginConfig) {
              return angular.equals(obj.artifact, latestArtifact);
            } else {
              return angular.equals(obj.artifact, vm.pluginConfig.artifact);
            }
          })[0];
          vm.onPluginVersionSelect();
      });
    }

    function createOnChangeHandler(field) {
      return function (value) {
        vm.pluginConfig.plugin.properties[field] = value;
      };
    }

    vm.onPluginVersionSelect = function () {
      vm.submitted = false;
      if (!vm.plugin) { return; }

      if (!vm.pluginConfig) {
        vm.pluginConfig = {
          _backendProperties: vm.plugin.properties,
          plugin: {
            name: vm.plugin.name,
            properties: {}
          },
          outputSchema: [
            {
              'name': 'etlSchemaBody',
              'schema': ''
            }
          ],
          lock: {}
        };
      } else {
        vm.pluginConfig._backendProperties = vm.plugin.properties;
        vm.pluginConfig.plugin.name = vm.plugin.name;
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
          vm.pluginDescription = vm.plugin.description || '';
          angular.forEach(vm.groupsConfig.groups, function (group) {
            angular.forEach(group.fields, function (field) {
              if (field.defaultValue) {
                vm.pluginConfig.plugin.properties[field.name] = vm.pluginConfig.plugin.properties[field.name] || field.defaultValue;
              }

              field.onChangeHandler = createOnChangeHandler(field.name);
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

            let arraySchemaFormat = [HydratorPlusPlusNodeService.getOutputSchemaObj(HydratorPlusPlusHydratorService.formatSchemaToAvro(configOutputSchema.implicitSchema))];

            vm.pluginConfig.outputSchema = arraySchemaFormat;
          }
        }, function error () {
          // When there is no config

          vm.noConfig = true;
          vm.configFetched = true;

        });
    };


    PluginTemplatesDirStore.registerOnChangeListener(() => {
      let mode = PluginTemplatesDirStore.getMode();
      let isCloseCommand = PluginTemplatesDirStore.getIsCloseCommand();
      let isSaveSuccessfull = PluginTemplatesDirStore.getIsSaveSuccessfull();
      if (isCloseCommand || isSaveSuccessfull) {
        return;
      }
      if (mode === 'edit') {
        vm.templateType = PluginTemplatesDirStore.getTemplateType();
        vm.pluginType = PluginTemplatesDirStore.getPluginType();
        vm.templateName = PluginTemplatesDirStore.getTemplateName();
        vm.pluginName = PluginTemplatesDirStore.getPluginName();
        vm.prefill = {
          templateType: true,
          pluginType: true,
          pluginName: true
        };
        vm.isEdit = true;

        mySettings.get('pluginTemplates')
          .then(function (res) {
            var template = res[$stateParams.namespace][vm.templateType][vm.pluginType][vm.templateName];
            vm.pluginConfig = {
              artifact: template.artifact,
              pluginTemplate: template.pluginTemplate,
              plugin: { properties: template.plugin ? template.plugin.properties : template.properties },
              outputSchema: template.outputSchema,
              lock: template.lock
            };

            oldTemplateName = template.pluginTemplate;
            vm.pluginDescription = template.description;

            initialize();
          });
      } else {
        vm.pluginName = PluginTemplatesDirStore.getPluginName();
        vm.prefill.pluginName = true;
        let templateType = PluginTemplatesDirStore.getTemplateType();
        let pluginType = PluginTemplatesDirStore.getPluginType();
        if (templateType && templateType.length && !vm.templateType) {
          vm.templateType = templateType;
          vm.prefill.templateType = true;
          vm.onPipelineTypeChange();
        }
        if (pluginType && pluginType.length && !vm.pluginType) {
          vm.pluginType = pluginType;
          vm.prefill.pluginType = true;
          vm.getPluginsList(pluginType);
        }
        vm.onPluginSelect();
      }
    });

    vm.cancel = function() {
      PluginTemplatesDirActions.templateClose(true);
    };

    vm.save = function () {
      if (!vm.pluginConfig.pluginTemplate) {
        vm.missingTemplateName = true;
        return;
      }

      var list = vm.pluginList.map(function (p) { return p.name; });
      if (list.indexOf(vm.pluginConfig.pluginTemplate) !== -1) {
        vm.pluginTemplateNameError = GLOBALS.en.admin.pluginSameNameError;
        return;
      }

      vm.loading = true;

      var outputPropertyName = myHelpers.objectQuery(vm.groupsConfig, 'outputSchema', 'outputSchemaProperty', '0');

      let outputSchema = myHelpers.objectQuery(vm.pluginConfig, 'outputSchema');
      if (outputPropertyName && vm.pluginConfig._backendProperties && vm.pluginConfig._backendProperties[outputPropertyName]) {
        if (Array.isArray(outputSchema)) {
          outputSchema = myHelpers.objectQuery(outputSchema, 0, 'schema');
        }

        vm.pluginConfig.plugin.properties[outputPropertyName] = outputSchema;
      }

      var properties = {
        artifact: vm.plugin.artifact,
        pluginTemplate: vm.pluginConfig.pluginTemplate,
        description: vm.pluginDescription,
        properties: vm.pluginConfig.plugin.properties,
        pluginType: vm.pluginType,
        templateType: vm.templateType,
        pluginName: vm.pluginName,
        outputSchema: outputSchema,
        lock: vm.pluginConfig.lock,
        nodeClass: 'plugin-templates'
      };

      var namespace = $stateParams.namespace;

      mySettings.get('pluginTemplates')
        .then(function(res) {

          if (!angular.isObject(res)) {
            res = {};
          }

          var config = myHelpers.objectQuery(res, namespace, properties.templateType, properties.pluginType, properties.pluginTemplate);

          if (config && !vm.isEdit) {
            vm.pluginTemplateNameError = GLOBALS.en.admin.templateNameExistsError;
            vm.loading = false;
            return;
          }

          if (vm.isEdit && oldTemplateName !== vm.pluginConfig.pluginTemplate) {
            if (config) {
              vm.pluginTemplateNameError = GLOBALS.en.admin.templateNameExistsError;
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

              PluginTemplatesDirActions.templateSavedSuccesfully();

            });
        });
    };

  });
