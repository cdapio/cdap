angular.module(PKG.name + '.commons')
  .directive('mySchemaEditor', function() {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
        config: '=',
        disabled: '='
      },
      templateUrl: 'widget-container/widget-schema-editor/widget-schema-editor.html',
      controller: function($scope, myHelpers) {
        var defaultOptions = [ 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string' ];
        var defaultType = null;
        if ($scope.config) {
          $scope.options = $scope.config['schema-types'];
          defaultType = $scope.config['schema-default-type'] || $scope.options[0];
        } else {
          $scope.options = defaultOptions;
          defaultType = 'string';
        }


        // Format model
        function initialize() {
          var schema = {};
          if ($scope.model) {
            try {
              schema = JSON.parse($scope.model);
            } catch (e) {
              $scope.error = 'Invalid JSON string';
            }
          }

          schema = myHelpers.objectQuery(schema, 'fields');
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
          // Note: 15 for now
          if ($scope.properties.length < 15) {
            if ($scope.properties.length === 0) {
              $scope.properties.push({
                name: '',
                type: defaultType,
                nullable: false
              });
            }

            for (var i = $scope.properties.length; i < 15; i++) {
              $scope.properties.push({
                empty: true
              });
            }
          }

        } // End of initialize

        initialize();

        $scope.$watch('disabled', function () {
          initialize();
        });


        function formatSchema() {
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
            var schema = {
              type: 'record',
              name: 'etlSchemaBody',
              fields: properties
            };
            // turn schema into JSON string
            var json = JSON.stringify(schema);

            $scope.model = json;
          } else {
            $scope.model = null;
          }

        }

        // watch for changes
        $scope.$watch('properties', formatSchema, true);

        $scope.emptyRowClick = function (property) {
          if (!property.empty) {
            return;
          }

          delete property.empty;
          property.name = '';
          property.type = defaultType;
          property.nullable = false;
        };

        $scope.addProperties = function() {
          $scope.properties.push({
            name: '',
            type: defaultType,
            nullable: false
          });
        };

        $scope.removeProperty = function(property) {
          var index = $scope.properties.indexOf(property);
          $scope.properties.splice(index, 1);

          $scope.properties.push({
            empty: true
          });
        };

        $scope.enter = function(event, last) {
          if (last && event.keyCode === 13) {
            $scope.addProperties();
          }
        };

      }
    };
  });
