angular.module(PKG.name + '.feature.adapters')
  .controller('PluginEditController', function($scope, PluginConfigFactory, myHelpers, EventPipe, $timeout, MyPlumbService, $sce, $rootScope) {
    var pluginCopy;

    var propertiesFromBackend = Object.keys($scope.plugin._backendProperties);
    // Make a local copy that is a mix of properties from backend + config from nodejs
    this.groups = {
      position: []
    };
    var missedFieldsGroup = {
      display: '',
      position: [],
      fields: {

      }
    };

    this.configfetched = null;
    this.properties = [];
    this.noconfig = null;
    if (MyPlumbService.metadata.template.type === 'ETLBatch') {
      this.infoPluginType = 'batch';
    } else if (MyPlumbService.metadata.template.type === 'ETLRealtime') {
      this.infoPluginType = 'real-time';
    }

    this.infoPluginCategory = $scope.plugin.type + 's';

    this.infoPluginName = $scope.plugin.name.toLowerCase();
    if (this.infoPluginCategory === 'transforms') {
      this.infoPluginCategory = 'transformations';
      this.infoUrl =
      'http://docs.cask.co/cdap/'
                      + $rootScope.cdapVersion + '/en/application-templates/etl/templates/'
                      + this.infoPluginCategory + '/'
                      + this.infoPluginName + '.html?hidenav';
    } else {
      this.infoUrl = 'http://docs.cask.co/cdap/'
                      + $rootScope.cdapVersion +'/en/application-templates/etl/templates/'
                      + this.infoPluginCategory + '/'
                      + this.infoPluginType + '/'
                      + this.infoPluginName + '.html?hidenav';
    }

    this.trustSrc = function(src) {
      return $sce.trustAsResourceUrl(src);
    };

    this.noproperty = Object.keys(
      $scope.plugin._backendProperties || {}
    ).length;

    if (this.noproperty) {
      PluginConfigFactory.fetch(
        $scope,
        $scope.type,
        $scope.plugin.name
      )
        .then(
          function success(res) {
            this.groups.position = res.groups.position;
            angular.forEach(
              res.groups.position,
              setGroups.bind(this, propertiesFromBackend, res)
            );

            // After iterating over all the groups check if the propertiesFromBackend is still empty
            // If not there are some fields from backend for which we don't have configuration from the nodejs.
            // Add them to the 'missedFieldsGroup' and show it as a separate group.
            if (propertiesFromBackend.length) {
              angular.forEach(
                propertiesFromBackend,
                setMissedFields.bind(this, missedFieldsGroup)
              );
              this.groups.position.push('generic');
              this.groups['generic'] = missedFieldsGroup;
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
              $scope.plugin.outputSchema = JSON.stringify(obj);
              $scope.plugin.implicitSchema = true;
            }

            // TODO: Hacky. Need to fix this for am-fade-top animation for modals.
            $timeout(function() {
              pluginCopy = angular.copy($scope.plugin);
              // Mark the configfetched to show that configurations have been received.
              this.configfetched = true;
              this.config = res;
              this.noconfig = false;
            }.bind(this), 1000);
          }.bind(this),
          function error() {
            // TODO: Hacky. Need to fix this for am-fade-top animation for modals.
            $timeout(function() {
              pluginCopy = angular.copy($scope.plugin);
              // Didn't receive a configuration from the backend. Fallback to all textboxes.
              this.noconfig = true;
              this.configfetched = true;
            }.bind(this), 1000);
          }.bind(this)
        );
    } else {
      this.configfetched = true;
    }

    function setGroups(propertiesFromBackend, res, group) {
      // For each group in groups iterate over its fields in position (order of all fields)
      var fieldsInGroup = res.groups[group].position;
      // Add an entry for the group in our local copy.
      this.groups[group] = {
        display: res.groups[group].display,
        position: [],
        fields: {}
      };
      angular.forEach(fieldsInGroup, setGroupFields.bind(this, propertiesFromBackend, res, group));
    }

    function setGroupFields(propertiesFromBackend, res, group, field) {
      // For each field in the group check if its been provided by the backend.
      // If yes add it to the local copy of groups
      // and mark the field as added.(remove from propertiesFromBackend array)
      var index = propertiesFromBackend.indexOf(field);
      if (index!== -1) {
        propertiesFromBackend.splice(index, 1);
        this.groups[group].position.push(field);
        this.groups[group].fields[field] = res.groups[group].fields[field];
        // If there is a description in the config from nodejs use that otherwise fallback to description from backend.
        var description = myHelpers.objectQuery(res, 'groups', group, 'fields', field, 'description');
        var info = myHelpers.objectQuery(this, 'groups', group, 'fields', field, 'info') ;
        var label = myHelpers.objectQuery(this, 'groups', group, 'fields', field, 'label');
        var defaultValue = myHelpers.objectQuery(this, 'groups', group, 'fields', field, 'properties', 'default');
        if (defaultValue && $scope.plugin.properties && $scope.plugin.properties.hasOwnProperty(field) && !$scope.plugin.properties[field]) {
          $scope.plugin.properties[field] = defaultValue;
        }

        if (!description || (description && !description.length)) {
          description = myHelpers.objectQuery($scope, 'plugin', '_backendProperties', field, 'description');
          this.groups[group].fields[field].description = description || 'No Description Available';
        }
        this.groups[group].fields[field].info = info || 'Info';
        if (!label) {
          this.groups[group].fields[field].label = field;
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

    this.reset = function () {
      $scope.plugin.properties = angular.copy(pluginCopy.properties);
      EventPipe.emit('plugin.reset');
    };

  });
