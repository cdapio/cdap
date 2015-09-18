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

angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceTemplatesController', function ($scope, myAdapterApi, PluginConfigFactory, myHelpers, mySettings, $stateParams, $alert, $state, GLOBALS, $rootScope) {

    var vm = this;

    vm.GLOBALS = GLOBALS;
    vm.pluginList = [];
    vm.isEdit = false;
    vm.isDisabled = false;
    vm.configFetched = false;
    vm.templationoptions = [
      vm.GLOBALS.etlBatch,
      vm.GLOBALS.etlRealtime
    ];
    vm.pluginoptions = [
      GLOBALS.pluginTypes[vm.GLOBALS.etlBatch].source,
      GLOBALS.pluginTypes[vm.GLOBALS.etlBatch].sink,
      GLOBALS.pluginTypes[vm.GLOBALS.etlBatch].transform
    ];

    var plugin;

    function initialize() {
      if (!vm.pluginName) { return; }

      var fetchApi;
      switch (vm.pluginType) {
        case GLOBALS.pluginTypes[vm.GLOBALS.etlBatch].source:
          fetchApi = myAdapterApi.fetchSourceProperties;
          break;
        case GLOBALS.pluginTypes[vm.GLOBALS.etlBatch].transform:
          fetchApi = myAdapterApi.fetchTransformProperties;
          break;
        case GLOBALS.pluginTypes[vm.GLOBALS.etlBatch].sink:
          fetchApi = myAdapterApi.fetchSinkProperties;
          break;
      }

      plugin = {
        name: vm.pluginName
      };

      var params = {
        namespace: $stateParams.nsadmin,
        adapterType: vm.templateType,
        extensionType: vm.pluginType,
        pluginName: vm.pluginName,
        version: $rootScope.cdapVersion
      };

      fetchApi(params).$promise
        .then(function (res) {
          var pluginProperties = (res.length? res[0].properties: {});

          plugin._backendProperties = pluginProperties;

          fetchConfig();
        });
    }

    if (!$stateParams.pluginTemplate) {
      // Obtaining list of plugins
      $scope.$watch(function () { return vm.pluginType; }, function () {
        if (!vm.pluginType) { return; }

        vm.pluginName = null;
        plugin = null;
        vm.pluginConfig = null;
        vm.configFetched = false;

        var prom;
        var params = {
          adapterType: vm.templateType,
          namespace: $stateParams.nsadmin,
          version: $rootScope.cdapVersion
        };
        switch (vm.pluginType) {
          case GLOBALS.pluginTypes[vm.GLOBALS.etlBatch].source:
            params.extensionType = GLOBALS.pluginTypes[vm.templateType].source;
            prom = myAdapterApi.fetchSources(params).$promise;
            break;
          case GLOBALS.pluginTypes[vm.GLOBALS.etlBatch].transform:
            params.extensionType = GLOBALS.pluginTypes[vm.templateType].transform;
            prom = myAdapterApi.fetchTransforms(params).$promise;
            break;
          case GLOBALS.pluginTypes[vm.GLOBALS.etlBatch].sink:
            params.extensionType = GLOBALS.pluginTypes[vm.templateType].sink;
            prom = myAdapterApi.fetchSinks(params).$promise;
            break;
        }
        prom.then(function (res) {
          vm.pluginList = res;
        });
      });

      // Fetching backend properties
      $scope.$watch(function () { return vm.pluginName; }, initialize);
    }


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
            pluginTemplate: template.pluginTemplate,
            properties: template.properties,
            outputSchema: template.outputSchema,
            lock: template.lock
          };

          initialize();

        });
    }

    vm.save = function () {

      if (!vm.pluginConfig.pluginTemplate) {
        $alert({
          type: 'danger',
          title: 'Error!',
          content: 'Please enter template name'
        });

        return;
      }

      var list = vm.pluginList.map(function (p) { return p.name; });
      if (list.indexOf(vm.pluginConfig.pluginTemplate) !== -1) {
        $alert({
          type: 'danger',
          title: 'Error!',
          content: 'There is already a plugin with the same name.'
        });

        return;
      }

      vm.loading = true;

      if (vm.pluginConfig._backendProperties && vm.pluginConfig._backendProperties.schema) {
        vm.pluginConfig.properties.schema = vm.pluginConfig.outputSchema;
      }

      var properties = {
        pluginTemplate: vm.pluginConfig.pluginTemplate,
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
            $alert({
              type: 'danger',
              content: 'Template name already exist! Please choose another name'
            });
            vm.loading = false;

            return;
          }

          var json = [
            namespace,
            properties.templateType,
            properties.pluginType,
            properties.pluginTemplate
          ].join('.');

          myHelpers.deepSet(res, json, properties);

          mySettings.set('pluginTemplates', res)
            .then(function () {
              $alert({
                type: 'success',
                content: 'Success saving template'
              });
              vm.loading = false;

              $state.go('admin.namespace.detail.templateslist');
            });
        });
    };



    function fetchConfig() {
      var propertiesFromBackend = Object.keys(plugin._backendProperties);
      var missedFieldsGroup = {
        display: '',
        position: [],
        fields: {

        }
      };

      vm.groups = {};
      PluginConfigFactory.fetch(
        $scope,
        vm.templateType,
        vm.pluginName
      )
        .then(
          function success(res) {
            if (res.schema) {
              vm.schemaProperties = res.schema;
            }

            vm.groups.position = res.groups.position;
            angular.forEach(
              res.groups.position,
              setGroups.bind(vm, propertiesFromBackend, res)
            );

            // After iterating over all the groups check if the propertiesFromBackend is still empty
            // If not there are some fields from backend for which we don't have configuration from the nodejs.
            // Add them to the 'missedFieldsGroup' and show it as a separate group.
            if (propertiesFromBackend.length) {
              angular.forEach(
                propertiesFromBackend,
                setMissedFields.bind(vm, missedFieldsGroup)
              );
              vm.groups.position.push('generic');
              vm.groups['generic'] = missedFieldsGroup;
            }

            if (res.implicit) {
              vm.isDisabled = true;
              var schema = res.implicit.schema;
              var keys = Object.keys(schema);

              var formattedSchema = [];
              angular.forEach(keys, function (key) {
                formattedSchema.push({
                  name: key,
                  type: schema[key]
                });
              });

              var obj = { fields: formattedSchema };
              plugin.outputSchema = JSON.stringify(obj);
              plugin.implicitSchema = true;
            }

            plugin.properties = {};
            plugin.lock = {};

            if (vm.isEdit) {
              plugin.properties = vm.pluginConfig.properties;
              plugin.pluginTemplate = vm.pluginConfig.pluginTemplate;
              plugin.outputSchema = vm.pluginConfig.outputSchema;
              plugin.lock = vm.pluginConfig.lock;
            }

            vm.pluginConfig = plugin;

            vm.configFetched = true;
          }
        );
    }

    function setGroups(propertiesFromBackend, res, group) {
      // For each group in groups iterate over its fields in position (order of all fields)
      var fieldsInGroup = res.groups[group].position;
      // Add an entry for the group in our local copy.
      vm.groups[group] = {
        display: res.groups[group].display,
        position: [],
        fields: {}
      };
      angular.forEach(fieldsInGroup, setGroupFields.bind(vm, propertiesFromBackend, res, group));
    }

    function setGroupFields(propertiesFromBackend, res, group, field) {
      // For each field in the group check if its been provided by the backend.
      // If yes add it to the local copy of groups
      // and mark the field as added.(remove from propertiesFromBackend array)
      var index = propertiesFromBackend.indexOf(field);
      if (index!== -1) {
        propertiesFromBackend.splice(index, 1);
        vm.groups[group].position.push(field);
        vm.groups[group].fields[field] = res.groups[group].fields[field];
        // If there is a description in the config from nodejs use that otherwise fallback to description from backend.
        var description = myHelpers.objectQuery(res, 'groups', group, 'fields', field, 'description');
        var info = myHelpers.objectQuery(vm, 'groups', group, 'fields', field, 'info') ;
        var label = myHelpers.objectQuery(vm, 'groups', group, 'fields', field, 'label');
        var defaultValue = myHelpers.objectQuery(vm, 'groups', group, 'fields', field, 'properties', 'default');
        if (defaultValue && plugin.properties && plugin.properties.hasOwnProperty(field) && plugin.properties[field]) {
          plugin.properties[field] = defaultValue;
        }

        if (!description || (description && !description.length)) {
          description = myHelpers.objectQuery(plugin, '_backendProperties', field, 'description');
          vm.groups[group].fields[field].description = description || 'No Description Available';
        }
        vm.groups[group].fields[field].info = info || 'Info';
        if (!label) {
          vm.groups[group].fields[field].label = field;
        }
      }
    }

    function setMissedFields (missedFieldsGroup, property) {
      missedFieldsGroup.position.push(property);
      missedFieldsGroup.fields[property] = {
        widget: 'textbox',
        label: property,
        info: 'Info',
        description: myHelpers.objectQuery($scope, 'plugin', '_backendProperties', property, 'description') || 'No Description Available'
      };
    }

  });
