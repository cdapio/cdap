angular.module(PKG.name + '.commons')
  .directive('mySchemaEditor', function($window) {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel'
      },
      templateUrl: 'widget-container/widget-schema-editor/widget-schema-editor.html',
      controller: function($scope, myHelpers, $timeout) {
        $scope.formatOptions = ['avro', 'clf', 'csv', 'grok', 'syslog', 'text', 'tsv'];

        // Format model
        function initialize() {
          var obj = {};
          if ($scope.model) {
            obj = JSON.parse($scope.model);
          }

          // need to happen after list of options are rendered
          $timeout(function() {
            $scope.format = obj.name ? obj.name : 'text';
          });

          var schema = myHelpers.objectQuery(obj, 'schema', 'fields');
          $scope.properties = [];
          angular.forEach(schema, function(p) {
            if (angular.isArray(p.type)) {
              $scope.properties.push({
                name: p.name,
                type: p.type[0],
                nullable: true
              });
            } else if (angular.isObject(p.type)) {
              $scope.properties.push({
                name: p.name,
                type: p.type.items,
                nullable: false
              });
            } else {
              $scope.properties.push({
                name: p.name,
                type: p.type,
                nullable: false
              });
            }
          });

          var settings = myHelpers.objectQuery(obj, 'settings');
          $scope.settings = [];
          angular.forEach(settings, function(v, k) {
            $scope.settings.push({
              key: k,
              value: v
            });
          });

        } // End of initialize

        initialize();


        function formatSchema() {
          var obj =  {
            name: $scope.format
          };

          // Format Schema
          var properties = [];
          angular.forEach($scope.properties, function(p) {
            if (p.name) {
              properties.push({
                name: p.name,
                type: p.nullable ? [p.type, 'null'] : p.type
              });
            }
          });

          // do not include properties on the request when schema field is empty
          if (properties.length !== 0) {
            obj.schema = {
              type: 'record',
              name: 'etlSchemaBody',
              fields: properties
            };
          }


          // Format Settings
          var settings = {};
          angular.forEach($scope.settings, function(v) {
            if (v.key) {
              settings[v.key] = v.value;
            }
          });
          // do not include settings on request when there is no setting defined
          if (Object.keys(settings).length !== 0) {
            obj.settings = settings;
          }

          // turn obj into JSON string
          var json = JSON.stringify(obj);

          $scope.model = json;
        }


        // watch for changes
        var propertiesListener = $scope.$watch('properties', function() {
          formatSchema();
        }, true);

        var settingListener = $scope.$watch('settings', function() {
          formatSchema();
        }, true);

        var formatListener = $scope.$watch('format', function() {
          formatSchema();
        });

        // remove watchers
        $scope.$on('$destroy', function() {
          propertiesListener();
          settingListener();
          formatListener();
        });


        $scope.addProperties = function() {
          $scope.properties.unshift({
            name: '',
            type: 'string',
            nullable: false
          });
        };

        $scope.removeProperty = function(property) {
          var index = $scope.properties.indexOf(property);
          $scope.properties.splice(index, 1);
        };

        $scope.addSetting = function() {
          $scope.settings.unshift({
            key: '',
            value: ''
          });
        };

        $scope.removeSetting = function(setting) {
          var index = $scope.settings.indexOf(setting);
          $scope.settings.splice(index, 1);
        };

      }
    };
  });