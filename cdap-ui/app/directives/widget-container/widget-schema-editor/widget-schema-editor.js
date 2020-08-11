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
      controller: function($scope, myHelpers, EventPipe, IMPLICIT_SCHEMA, HydratorPlusPlusHydratorService, $timeout, myAlertOnValium) {
        $scope.limitedToView = 15;
        var typeMap = 'map<string, string>';

        var defaultOptions = [ 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string', 'map<string, string>' ];
        var defaultType = null;
        var watchProperty = null;
        var schemaExportTimeout = null;

        var eventEmitter = window.CaskCommon.ee(window.CaskCommon.ee);

        $scope.fields = 'SHOW';

        if ($scope.config) {

          $scope.options = myHelpers.objectQuery($scope.config, 'widget-attributes', 'schema-types') || myHelpers.objectQuery($scope.config, 'schema-types');
          defaultType = myHelpers.objectQuery($scope.config, 'widget-attributes', 'schema-default-type') || myHelpers.objectQuery($scope.config, 'schema-default-type') || $scope.options[0];
          watchProperty = myHelpers.objectQuery($scope.config, 'property-watch') || myHelpers.objectQuery($scope.config, 'widget-attributes', 'property-watch');
          if (watchProperty) {

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

          var availableImplicitSchema = Object.keys(IMPLICIT_SCHEMA);

          // do things based on format
          if (availableImplicitSchema.concat(['']).indexOf($scope.pluginProperties[watchProperty]) > -1 ) {
            $scope.model = null;
            $scope.disableEdit = true;
            $scope.pluginProperties['format.setting.pattern'] = null;

            if (availableImplicitSchema.indexOf($scope.pluginProperties[watchProperty]) > -1) {
              var implicitSchema = IMPLICIT_SCHEMA[$scope.pluginProperties[watchProperty]];

              initialize(implicitSchema);
            } else {
              $scope.fields = 'NOTHING';
            }

          } else if ($scope.pluginProperties[watchProperty] === 'avro'){
            $scope.disableEdit = false;
            $scope.fields = 'AVRO';
            $scope.pluginProperties['format.setting.pattern'] = null;
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
              $scope.pluginProperties['format.setting.pattern'] = $scope.grok.pattern;
            }, true);
          }

          else {
            $scope.disableEdit = false;
            $scope.fields = 'SHOW';
            $scope.pluginProperties['format.setting.pattern'] = null;
            initialize($scope.model);
          }
        }


        var filledCount;

        // Format model
        function initialize(jsonString) {
          filledCount = 0;
          var schema = {};
          $scope.avro = {};
          $scope.grok = {
            pattern: $scope.pluginProperties['format.setting.pattern']
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
          $scope.activeCell = false;

          angular.forEach(schema, function(p) {
            if (angular.isArray(p.type)) {
              var mapType = p.type[0];

              if (mapType.type === 'map') {
                mapType = mapType.keys === 'string' && mapType.values === 'string' ? typeMap : null;
              }

              $scope.properties.push({
                name: p.name,
                type: mapType,
                nullable: true,
                readonly: p.readonly
              });
            } else if (angular.isObject(p.type)) {
              if (p.type.type === 'map') {
                $scope.properties.push({
                  name: p.name,
                  type: p.type.keys === 'string' && p.type.values === 'string' ? typeMap : null,
                  nullable: p.nullable,
                  readonly: p.readonly
                });
              } else {
                $scope.properties.push({
                  name: p.name,
                  type: p.type.items,
                  nullable: p.nullable,
                  readonly: p.readonly
                });
              }
            } else {
              $scope.properties.push({
                name: p.name,
                type: p.type,
                nullable: p.nullable,
                readonly: p.readonly
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

        if ($scope.config && $scope.config['property-watch']) {
          changeFormat();
        }

        initialize($scope.model);

        function onSchemaClear() {
          var schema;
          try {
            schema = JSON.parse($scope.model);
            schema.fields = schema.fields.filter(function (value) {
              return value.readonly;
            });
            $scope.model = JSON.stringify(schema);
          } catch (e) {
            $scope.model = JSON.stringify({ fields: [] });
          }
          initialize($scope.model);
        }

        eventEmitter.on('schema.clear', onSchemaClear);

        EventPipe.on('dataset.selected', function (schema) {
          try {
            var a = JSON.parse($scope.model).fields.filter(function(e) {
              return !e.readonly;
            });
            if (a.length) {
              throw 'Model already set';
            }
          } catch (n) {
              if ($scope.model) { return; }
          }

          var modSchema = {fields: []};
          try {
            modSchema.fields = JSON.parse($scope.model).fields.filter(function(field) {
              return field.readonly;
            });
            modSchema.fields = modSchema.fields.concat(JSON.parse(schema).fields);
          } catch(e) {
            modSchema = schema;
          }
          initialize(JSON.stringify(modSchema));
        });


        function formatAvro() {
          if ($scope.pluginProperties[watchProperty] !== 'avro') {
            return;
          }

          var avroJson = JSON.stringify($scope.avro.schema);
          $scope.model = avroJson;
        }


        function formatSchema() {
          $scope.model = HydratorPlusPlusHydratorService.formatOutputSchema($scope.properties);
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

        function exportSchema() {
          if ($scope.url) {
            URL.revokeObjectURL($scope.url);
          }

          var schema = JSON.parse($scope.model);
          schema = schema.fields;

          angular.forEach(schema, function (field) {
            if (field.readonly) {
              delete field.readonly;
            }
          });

          var blob = new Blob([JSON.stringify(schema, null, 4)], { type: 'application/json'});
          $scope.url = URL.createObjectURL(blob);
          $scope.exportFileName = 'schema';

          if (schemaExportTimeout) {
            $timeout.cancel(schemaExportTimeout);
          }

          schemaExportTimeout = $timeout(function() {
            document.getElementById('schema-export-link').click();
          });
        }

        function onSchemaImport(data) {
          var fields = [];
          try {
            fields = JSON.parse(data);
          } catch (e) {
            myAlertOnValium.show({
              type: 'danger',
              content: 'Error parsing imported schema'
            });
          }

          var schema = {
            fields: fields
          };
          initialize(JSON.stringify(schema));
        }

        eventEmitter.on('schema.export', exportSchema);
        eventEmitter.on('schema.import', onSchemaImport);

        $scope.$on('$destroy', function() {
          eventEmitter.off('schema.import', onSchemaImport);
          eventEmitter.off('schema.clear', onSchemaClear);
          eventEmitter.off('schema.export', exportSchema);
          EventPipe.cancelEvent('dataset.selected');
          URL.revokeObjectURL($scope.url);

          if (schemaExportTimeout) {
            $timeout.cancel(schemaExportTimeout);
          }
        });

        $scope.loadNextSetOfRows = function() {
          $scope.limitedToView = $scope.limitedToView + 10;
        };
      }
    };
  });
