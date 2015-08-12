angular.module(PKG.name + '.feature.admin')
  .controller('NamespaceTemplatesController', function ($scope, myAdapterApi, PluginConfigFactory, myHelpers, mySettings, $stateParams) {

    var vm = this;

    vm.pluginList = [];

    var fetchApi;

    var plugin;

    // Obtaining list of plugins
    $scope.$watch(function () { return vm.pluginType; }, function () {
      if (!vm.pluginType) { return; }

      vm.plugin = null;
      plugin = null;
      vm.pluginConfig = null;

      var prom;
      switch (vm.pluginType) {
        case 'source':
          prom = myAdapterApi.fetchSources({ adapterType: vm.template }).$promise;
          fetchApi = myAdapterApi.fetchSourceProperties;
          break;
        case 'transform':
          prom = myAdapterApi.fetchTransforms({ adapterType: vm.template }).$promise;
          fetchApi = myAdapterApi.fetchTransformProperties;
          break;
        case 'sink':
          prom = myAdapterApi.fetchSinks({ adapterType: vm.template }).$promise;
          fetchApi = myAdapterApi.fetchSinkProperties;
          break;
      }
      prom.then(function (res) {
        vm.pluginList = res;
      });
    });


    // Fetching backend properties
    $scope.$watch(function () { return vm.plugin; }, function () {
      console.log('vm.plugin', vm.plugin);
      if (!vm.plugin) { return; }

      plugin = {
        name: vm.plugin
      };

      var params = {
        adapterType: vm.template
      };
      params[vm.pluginType] = vm.plugin;

      fetchApi(params).$promise
        .then(function (res) {
          var pluginProperties = (res.length? res[0].properties: {});

          plugin._backendProperties = pluginProperties;

          fetchConfig();
        });
    });

    this.save = function () {
      var properties = {
        templateName: vm.pluginConfig.templateName,
        properties: vm.pluginConfig.properties,
        type: vm.template,
        pluginType: vm.pluginType,
        pluginName: vm.plugin
      };

      console.log('PROPERTIES', properties);

      var namespace = $stateParams.nsadmin;

      mySettings.get('pluginTemplates')
        .then(function(res) {
          console.log('res', res);

          if (!angular.isObject(res)) {
            res = {};
          }

          if (!res[namespace]) {
            res[namespace] = {};
          }

          res[namespace][properties.templateName] = properties;
          return mySettings.set('pluginTemplates', res);
        })
        .then(function (asd) {
          mySettings.get('pluginTemplates').then(function (aaaa) { console.log('aaa', aaaa); } );
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
        vm.template,
        vm.plugin
      )
        .then(
          function success(res) {
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

            vm.pluginConfig = plugin;
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
