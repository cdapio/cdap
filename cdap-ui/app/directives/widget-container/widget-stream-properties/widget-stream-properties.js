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
      controller: function($scope, EventPipe) {
        var modelCopy = angular.copy()

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

        var filledCount;

        // Format model
        function initialize() {
          filledCount = 0;
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
        EventPipe.on('plugin.reset', function () {
          $scope.model = angular.copy(modelCopy);

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

        function formatAvro() {
          if ($scope.plugins.format !== 'avro') {
            return;
          }

          var avroJson = JSON.stringify($scope.avro.schema);
          $scope.model = avroJson;
        }

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
        };

        $scope.enter = function(event, last) {
          if (last && event.keyCode === 13) {
            $scope.addProperties();
          }
        };

      }
    };
  });
