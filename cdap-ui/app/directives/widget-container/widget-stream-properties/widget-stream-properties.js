angular.module(PKG.name + '.commons')
  .directive('myStreamProperties', function() {
    return {
      restrict: 'E',
      scope: {
        model: '=ngModel',
        config: '=',
        plugins: '='
      },
      templateUrl: 'widget-container/widget-stream-properties/widget-stream-properties.html',
      controller: function($scope) {
        var defaultOptions = [ 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string' ];
        var defaultType = null;
        if ($scope.config) {
          $scope.options = $scope.config['schema-types'];
          defaultType = $scope.config['schema-default-type'] || $scope.options[0];
        } else {
          $scope.options = defaultOptions;
          defaultType = 'string';
        }

        var watcher;
        function removeWatcher() {
          if (watcher) {
            // deregister watch
            watcher();
            watcher = null;
          }
        }

        $scope.$watch('plugins.format', function() {
          if (!$scope.plugins) {
            return;
          }

          // watch for changes
          removeWatcher();

          // do things based on format
          if (['clf', 'syslog', ''].indexOf($scope.plugins.format) > -1) {
            $scope.model = null;
            $scope.fields = 'NOTHING';
          } else if ($scope.plugins.format === 'avro'){
            $scope.fields = 'AVRO';
            watcher = $scope.$watch('avro', formatAvro, true);

            if ($scope.model) {
              try {
                $scope.avro.schema = JSON.parse($scope.model);
              } catch (e) {
                $scope.error = 'Invalid JSON string';
              }

            }
          } else if ($scope.plugins.format === 'grok') {
            $scope.fields = 'GROK';
            $scope.model = null;
          }

          else {
            $scope.fields = 'SHOW';
            watcher = $scope.$watch('properties', formatSchema, true);
          }

        });

        var filledCount = 0;

        // Format model
        function initialize() {
          var schema = {};
          $scope.avro = {};

          if ($scope.model) {
            try {
              schema = JSON.parse($scope.model);
            } catch (e) {
              $scope.error = 'Invalid JSON string';
            }
          }

          schema = schema.fields;
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
          } else { // to add one empty line when there are more than 15
            $scope.properties.push({
              empty: true
            });
          }

        } // End of initialize

        initialize();


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

        function formatAvro() {
          if ($scope.plugins.format !== 'avro') {
            return;
          }

          var avroJson = JSON.stringify($scope.avro.schema);
          $scope.model = avroJson;
        }

        $scope.emptyRowClick = function (property, index) {
          if (!property.empty || index !== filledCount || $scope.disabled) {
            return;
          }

          delete property.empty;
          property.name = '';
          property.type = defaultType;
          property.nullable = false;
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
            nullable: false
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
