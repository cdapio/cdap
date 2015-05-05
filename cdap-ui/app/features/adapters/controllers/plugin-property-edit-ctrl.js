angular.module(PKG.name + '.feature.adapters')
  .controller('PluginEditController', function($scope, PluginConfigFactory, myHelpers) {
    var propertiesFromBackend = Object.keys($scope.plugin.properties);
    // Make a local copy that is a mix of properties from backend + config from nodejs
    $scope.groups = {
      position: []
    };
    var missedFieldsGroup = {
      display: '',
      position: [],
      fields: {

      }
    };

    $scope.configfetched = false;
    $scope.properties = [];
    $scope.noconfig = false;
    $scope.noproperty = Object.keys(
      $scope.plugin.properties || {}
    ).length;



    if ($scope.noproperty) {
      PluginConfigFactory.fetch($scope, $scope.$parent.metadata.type, $scope.plugin.name)
        .then(
          function success(res) {
            $scope.groups.position = res.groups.position;
            angular.forEach(res.groups.position, function(group) {
              // For each group in groups iterate over its fields in position (order of all fields)
              var fieldsInGroup = res.groups[group].position;
              // Add an entry for the group in our local copy.
              $scope.groups[group] = {
                display: res.groups[group].display,
                position: [],
                fields: {}
              };
              angular.forEach(fieldsInGroup, function(field) {
                // For each field in the group check if its been provided by the backend.
                // If yes add it to the local copy of groups
                // and mark the field as added.(remove from propertiesFromBackend array)
                var index = propertiesFromBackend.indexOf(field);
                if (index!== -1) {
                  propertiesFromBackend.splice(index, 1);
                  $scope.groups[group].position.push(field);
                  $scope.groups[group].fields[field] = res.groups[group].fields[field];
                  // If there is a description in the config from nodejs use that otherwise fallback to description from backend.

                  if (!myHelpers.objectQuery(res, 'groups', group, 'fields', field, 'description', 'length')) {
                    $scope.groups[group].fields[field].description = myHelpers.objectQuery($scope, 'plugin', '_backendProperties', field, 'description') || 'No Description Available';
                  }
                  $scope.groups[group].fields[field].info = myHelpers.objectQuery($scope, 'groups', group, 'fields', field, 'info') || 'Info';
                  if (!myHelpers.objectQuery($scope, 'groups', group, 'fields', field, 'label')) {
                    $scope.groups[group].fields[field].label = field;
                  }
                }
              });
            });

            // After iterating over all the groups check if the propertiesFromBackend is still empty
            // If not there are some fields from backend for which we don't have configuration from the nodejs.
            // Add them to the 'missedFieldsGroup' and show it as a separate group.
            if (propertiesFromBackend.length) {
              angular.forEach(propertiesFromBackend, function(property) {
                missedFieldsGroup.position.push(property);
                missedFieldsGroup.fields[property] = {
                  widget: 'textbox',
                  label: property,
                  info: 'Info',
                  description: myHelpers.objectQuery($scope, 'plugin', '_backendProperties', property, 'description') || 'No Description Available'
                };
              });
              $scope.groups.position.push('generic');
              $scope.groups['generic'] = missedFieldsGroup;
            }

            // Mark the configfetched to show that configurations have been received.
            $scope.configfetched = true;
            $scope.config = res;
          },
          function error(err) {
            // Didn't receive a configuration from the backend. Fallback to all textboxes.
            $scope.noconfig = true;
          }
        );
    }
  });
