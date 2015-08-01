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
      controller: function($scope, myHelpers, EventPipe) {
        var typeMap = 'map<string, string>';
        var mapObj = {
          type: 'map',
          keys: 'string',
          values: 'string'
        };

        var defaultOptions = [ 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string', 'map<string, string>' ];
        var defaultType = null;
        if ($scope.config) {
          $scope.options = $scope.config['schema-types'];
          defaultType = $scope.config['schema-default-type'] || $scope.options[0];
        } else {
          $scope.options = defaultOptions;
          defaultType = 'string';
        }

        var modelCopy = angular.copy($scope.model);

        EventPipe.on('plugin.reset', function () {
          $scope.model = angular.copy(modelCopy);
          initialize();
        });

        var filledCount;

        // Format model
        function initialize() {
          filledCount = 0;

          var schema = {};
          $scope.error = null;
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
              if (p.type.type === 'map') {
                $scope.properties.push({
                  name: p.name,
                  type: typeMap,
                  nullable: p.nullable
                });
              } else {
                $scope.properties.push({
                  name: p.name,
                  type: p.type.items,
                  nullable: p.nullable
                });
              }
            } else {
              $scope.properties.push({
                name: p.name,
                type: p.type,
                nullable: p.nullable
              });
            }
          });

          filledCount = $scope.properties.length;

          // Note: 15 for now
          if ($scope.properties.length < 15) {
            if ($scope.properties.length === 0) {
              $scope.properties.push({
                name: '',
                type: defaultType,
                nullable: false
              });
              filledCount = 1;
            }

            for (var i = $scope.properties.length; i < 15; i++) {
              $scope.properties.push({
                empty: true
              });
            }
          } else { // to add one empty line when there are more than 15 fields
            $scope.properties.push({
              empty: true
            });
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
              var property;
              if (p.type === typeMap) {
                property = angular.copy(mapObj);
              } else {
                property = p.type;
              }

              properties.push({
                name: p.name,
                type: p.nullable ? [property, 'null'] : property
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

        $scope.emptyRowClick = function (property, index) {
          if (!property.empty || index !== filledCount || $scope.disabled) {
            return;
          }

          delete property.empty;
          property.name = '';
          property.type = defaultType;
          property.nullable = false;
          property.newField = 'add';
          filledCount++;

          if (filledCount >= 15) {
            $scope.properties.push({
              empty: true
            });
          }
        };

        $scope.addProperties = function() {
          $scope.properties.push({
            name: '',
            type: defaultType,
            nullable: false,
            newField: 'add'
          });

          filledCount++;

          if ($scope.properties.length >= 15) {
            $scope.properties.push({
              empty: true
            });
          }
        };

        $scope.removeProperty = function(property) {
          var index = $scope.properties.indexOf(property);
          $scope.properties.splice(index, 1);

          if ($scope.properties.length <= 15) {
            $scope.properties.push({
              empty: true
            });
          }
          filledCount--;
        };

        $scope.enter = function(event, index) {
          if (index === filledCount-1 && event.keyCode === 13) {
            if (filledCount < $scope.properties.length) {
              $scope.emptyRowClick($scope.properties[index + 1], index+1);
            } else {
              $scope.addProperties();
            }
          }
        };

      }
    };
  });
