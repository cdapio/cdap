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

angular.module(PKG.name + '.commons')
  .directive('mySchemaEditor', function() {
    return {
      restrict: 'EA',
      scope: {
        model: '=ngModel',
        config: '=',
        pluginProperties: '=',
        disabled: '='
      },
      templateUrl: 'widget-container/widget-schema-editor/widget-schema-editor.html',
      controller: function($scope, myHelpers, EventPipe, IMPLICIT_SCHEMA) {
        var modelCopy = angular.copy($scope.model);

        var typeMap = 'map<string, string>';
        var mapObj = {
          type: 'map',
          keys: 'string',
          values: 'string'
        };

        var defaultOptions = [ 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string', 'map<string, string>' ];
        var defaultType = null;
        var watchProperty = null;

        $scope.fields = 'SHOW';

        if ($scope.config) {

          $scope.options = $scope.config['schema-types'];
          defaultType = $scope.config['schema-default-type'] || $scope.options[0];

          if ($scope.config['property-watch']) {
            watchProperty = $scope.config['property-watch'];

            // changing the format when it is stream
            EventPipe.on('dataset.selected', function (schema, format) {
              $scope.pluginProperties[watchProperty] = format;
            });

            $scope.$watch(function () {
              return $scope.pluginProperties[watchProperty];
            }, changeFormat);
          }

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

        function changeFormat() {
          if (!$scope.pluginProperties) {
            return;
          }

          // watch for changes
          removeWatcher();

          // do things based on format
          if (['clf', 'syslog', ''].indexOf($scope.pluginProperties[watchProperty]) > -1) {
            $scope.model = null;
            $scope.disableEdit = true;
            // $scope.fields = 'NOTHING';
            if ($scope.pluginProperties[watchProperty] === 'clf') {
              var clfSchema = IMPLICIT_SCHEMA.clf;

              initialize(clfSchema);
              $scope.fields = 'SHOW';
            } else if ($scope.pluginProperties[watchProperty] === 'syslog') {
              var syslogSchema = IMPLICIT_SCHEMA.syslog;

              initialize(syslogSchema);
              $scope.fields = 'SHOW';
            } else {
              $scope.fields = 'NOTHING';
            }

          } else if ($scope.pluginProperties[watchProperty] === 'avro'){
            $scope.disableEdit = false;
            $scope.fields = 'AVRO';
            watcher = $scope.$watch('avro', formatAvro, true);

            if ($scope.model) {
              try {
                $scope.avro.schema = JSON.parse($scope.model);
              } catch (e) {
                $scope.error = 'Invalid JSON string';
              }

            }
          } else if ($scope.pluginProperties[watchProperty] === 'grok') {
            $scope.disableEdit = false;
            $scope.fields = 'GROK';
            watcher = $scope.$watch('grok', function () {
              $scope.model = $scope.grok.pattern;
            }, true);
          }

          else {
            $scope.disableEdit = false;
            $scope.fields = 'SHOW';
            watcher = $scope.$watch('properties', formatSchema, true);
          }
        }


        var filledCount;

        // Format model
        function initialize(jsonString) {
          filledCount = 0;
          var schema = {};
          $scope.avro = {};
          $scope.grok = {
            pattern: $scope.model
          };

          $scope.error = null;
          if (jsonString) {
            try {
              schema = JSON.parse(jsonString);
              $scope.avro.schema = schema;
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
          formatSchema();

        } // End of initialize

        initialize($scope.model);

        EventPipe.on('plugin.reset', function () {
          $scope.model = angular.copy(modelCopy);
          initialize($scope.model);
        });

        EventPipe.on('schema.clear', function () {
          initialize();
        });

        EventPipe.on('dataset.selected', function (schema) {
          initialize(schema);
        });

        $scope.$watch('disabled', function () {
          initialize($scope.model);
        });

        function formatAvro() {
          if ($scope.pluginProperties[watchProperty] !== 'avro') {
            return;
          }

          var avroJson = JSON.stringify($scope.avro.schema);
          $scope.model = avroJson;
        }


        function formatSchema() {
          if (watchProperty && $scope.pluginProperties && ['clf', 'syslog'].indexOf($scope.pluginProperties[watchProperty]) !== -1) {
            $scope.model = null;
            return;
          }

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
