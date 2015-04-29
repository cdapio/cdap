angular.module(PKG.name + '.commons')
  .directive('mySchemaEditor', function($window) {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
        config: '='
      },
      templateUrl: 'widget-container/widget-schema-editor/widget-schema-editor.html',
      controller: function($scope, myHelpers) {
        $scope.options = $scope.config['schema-types'];
        var defaultType = $scope.config['schema-default-type'] || $scope.options[0];

        // Format model
        function initialize() {
          var obj = {};

          if ($scope.model) {
            try {
              obj = JSON.parse($scope.model);
            } catch (e) {
              $scope.error = "Invalid JSON string";
            }
          }

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

          // turn obj into JSON string
          var json = JSON.stringify(obj);

          $scope.model = json;
        }

        // watch for changes
        $scope.$watch('properties', formatSchema, true);


        $scope.addProperties = function() {
          $scope.properties.unshift({
            name: '',
            type: defaultType,
            nullable: false
          });
        };

        $scope.removeProperty = function(property) {
          var index = $scope.properties.indexOf(property);
          $scope.properties.splice(index, 1);
        };

      }
    };
  });
