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

angular.module(PKG.name + '.feature.adapters')
  .controller('PluginEditController', function($scope, PluginConfigFactory, myHelpers, EventPipe, $timeout, MyAppDAGService, GLOBALS, MyNodeConfigService) {
    $scope.pluginCopy = {};

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

    this.configfetched = false;
    this.properties = [];
    this.noconfig = null;
    this.MyNodeConfigService = MyNodeConfigService;
    /////////////////////////////////////
    /*
     This is snippet is here because the widget-schema-editor edits the outputschema that we pass in
     which makes the plugin.outputschema to be always edited when the user opens the plugin for the first time.
     So even though the user didn't actually do anything this behavior was forcing the user to save the node when opened for the first time.

     So we are temporarily doing the coversion here before passing it to the widget-schema-editor so that it doesn't trigger that initial change.
     This should eventually be removed and moved to a service (or a factory). For lack of time my sin stays here. - Ajai
    */
    if (!$scope.isDisabled) {
      MyNodeConfigService.setIsPluginBeingEdited(false);
    }

    var typeMap = 'map<string, string>';
    var mapObj = {
      type: 'map',
      keys: 'string',
      values: 'string'
    };

    function constructOutputSchema(jsonBlob, config, pluginProperties) {
      var options, defaultType;
      var defaultOptions = [ 'boolean', 'int', 'long', 'float', 'double', 'bytes', 'string', 'map<string, string>' ];
      if (config){
        options = config['schema-types'];
        defaultType = config['schema-default-type'] || options[0];
      } else {
        options = defaultOptions;
        defaultType = 'string';
      }

      var schema = jsonBlob;
      schema = myHelpers.objectQuery(schema, 'fields');
      var properties = [];
      angular.forEach(schema, function(p) {
        if (angular.isArray(p.type)) {
          properties.push({
            name: p.name,
            type: p.type[0],
            nullable: true
          });
        } else if (angular.isObject(p.type)) {
          if (p.type.type === 'map') {
            properties.push({
              name: p.name,
              type: typeMap,
              nullable: p.nullable
            });
          } else {
            properties.push({
              name: p.name,
              type: p.type.items,
              nullable: p.nullable
            });
          }
        } else {
          properties.push({
            name: p.name,
            type: p.type,
            nullable: p.nullable
          });
        }
      });

      // Note: 15 for now
      if (properties.length < 15) {
        if (properties.length === 0) {
          properties.push({
            name: '',
            type: defaultType,
            nullable: false
          });
        }

        for (var i = properties.length; i < 15; i++) {
          properties.push({
            empty: true
          });
        }
      } else { // to add one empty line when there are more than 15 fields
        properties.push({
          empty: true
        });
      }

      return formatSchema(properties, config, pluginProperties);
    }

    function formatSchema(properties, config, pluginProperties) {

      if (config && config['property-watch'] && pluginProperties && ['clf', 'syslog'].indexOf(pluginProperties[config['property-watch']]) !== -1) {
        return null;
      }

      // Format Schema
      var prop = [];
      angular.forEach(properties, function(p) {
        if (p.name) {
          var property;
          if (p.type === typeMap) {
            property = angular.copy(mapObj);
          } else {
            property = p.type;
          }

          prop.push({
            name: p.name,
            type: p.nullable ? [property, 'null'] : property
          });
        }
      });

      // do not include properties on the request when schema field is empty
      if (prop.length !== 0) {
        var schema = {
          type: 'record',
          name: 'etlSchemaBody',
          fields: prop
        };
        // turn schema into JSON string
        return JSON.stringify(schema);
      } else {
        return null;
      }
    }
    /////////////////////////////////////

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
            var jsonBlob;
            if (res.schema) {
              this.schemaProperties = res.schema;
            }

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
              $scope.plugin.outputSchema = constructOutputSchema(obj, this.schemaProperties, $scope.plugin.properties);
              $scope.plugin.implicitSchema = true;
            } else {
              try {
                jsonBlob = JSON.parse($scope.plugin.outputSchema);
              } catch(e) {
                console.log('ERROR: ', e);
              }
              $scope.plugin.outputSchema = constructOutputSchema(jsonBlob, this.schemaProperties, $scope.plugin.properties);
            }
            if ($scope.plugin._backendProperties.schema) {
              if ($scope.plugin.properties.schema !== $scope.plugin.outputSchema) {
                $scope.plugin.properties.schema = $scope.plugin.outputSchema;
              }
            }
            $scope.pluginCopy = angular.copy($scope.plugin);

            // Mark the configfetched to show that configurations have been received.
            this.configfetched = true;
            this.config = res;
            this.noconfig = false;

            if (!$scope.isDisabled) {
              setWatchersForPropertiesAndSchema();
            }
          }.bind(this),
          function error(err) {
            // TODO: Hacky. Need to fix this for am-fade-top animation for modals.
            $timeout(function() {
              $scope.pluginCopy = angular.copy($scope.plugin);
              if (!$scope.isDisabled) {
                $scope.$watch('pluginCopy.properties', function() {
                    var strProp1 = JSON.stringify($scope.pluginCopy.properties);
                    var strProp2 = JSON.stringify($scope.plugin.properties);
                    if (strProp1 !== strProp2) {
                      MyNodeConfigService.setIsPluginBeingEdited(true);
                    }
                }, true);
              }
              // Didn't receive a configuration from the backend. Fallback to all textboxes.
              switch(err) {
                case 'NO_JSON_FOUND':
                  this.noConfigMessage = GLOBALS.en.hydrator.studio.noConfigMessage;
                  break;
                case 'CONFIG_SYNTAX_JSON_ERROR':
                  this.noConfigMessage = GLOBALS.en.hydrator.studio.syntaxConfigJsonError;
                  break;
                case 'CONFIG_SEMANTICS_JSON_ERROR':
                  this.noConfigMessage = GLOBALS.en.hydrator.studio.semanticConfigJsonError;
                  break;
              }
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

    function setWatchersForPropertiesAndSchema() {
      if ($scope.plugin._backendProperties.schema) {
        $scope.$watch('pluginCopy.outputSchema', function () {
          if (!$scope.pluginCopy.outputSchema) {
            if ($scope.pluginCopy.properties && $scope.plugin.properties.schema) {
              $scope.pluginCopy.properties.schema = null;
            }
            return;
          }

          if (!$scope.pluginCopy.properties) {
            $scope.pluginCopy.properties = {};
          }
          if ($scope.pluginCopy.properties.schema !== $scope.pluginCopy.outputSchema) {
            $scope.pluginCopy.properties.schema = $scope.pluginCopy.outputSchema;
          }
        });
      } else {
        $scope.$watch('pluginCopy.outputSchema', function () {
          var originalOutputSchema = $scope.plugin.outputSchema;
          var copyOutputSchema = $scope.pluginCopy.outputSchema;
          if (originalOutputSchema !== copyOutputSchema) {
            MyNodeConfigService.setIsPluginBeingEdited(true);
          }
        });
      }

      $scope.$watch('pluginCopy.label', function() {
        var copyLabel = $scope.pluginCopy.label;
        var originalLabel = $scope.plugin.label;
        if (copyLabel !== originalLabel) {
          MyNodeConfigService.setIsPluginBeingEdited(true);
        }
      });

      $scope.$watch('pluginCopy.errorDatasetName', function() {
        var copyErrorDataset = $scope.pluginCopy.errorDatasetName;
        var originalErrorDataset = $scope.plugin.errorDatasetName;
        if (copyErrorDataset !== originalErrorDataset) {
          MyNodeConfigService.setIsPluginBeingEdited(true);
        }
      });

      $scope.$watch('pluginCopy.validationFields', function() {
        var copyValidationFields = JSON.stringify($scope.pluginCopy.validationFields);
        var originalValidationFields = JSON.stringify($scope.plugin.validationFields);
        if (copyValidationFields !== originalValidationFields) {
          MyNodeConfigService.setIsPluginBeingEdited(true);
        }
      });

      $scope.$watch('pluginCopy.properties', function() {
          var strProp1 = JSON.stringify($scope.pluginCopy.properties);
          var strProp2 = JSON.stringify($scope.plugin.properties);
          if (strProp1 !== strProp2) {
            MyNodeConfigService.setIsPluginBeingEdited(true);
          }
      }, true);
    }

    this.reset = function () {
      $scope.pluginCopy.properties = angular.copy($scope.plugin.properties);
      $scope.pluginCopy.outputSchema = angular.copy($scope.plugin.outputSchema);
      $scope.pluginCopy.errorDatasetName = angular.copy($scope.plugin.errorDatasetName);
      EventPipe.emit('resetValidatorValidationFields', $scope.plugin.validationFields);
      MyNodeConfigService.setIsPluginBeingEdited(false);
      EventPipe.emit('plugin.reset');
    };

    this.schemaClear = function () {
      EventPipe.emit('schema.clear');
    };

    this.save = function () {
      if (validateSchema() || $scope.plugin.name === 'Validator') {
        $scope.plugin.properties = angular.copy($scope.pluginCopy.properties);
        $scope.plugin.outputSchema = angular.copy($scope.pluginCopy.outputSchema);
        $scope.plugin.errorDatasetName = $scope.pluginCopy.errorDatasetName;
        $scope.plugin.validationFields = angular.copy($scope.pluginCopy.validationFields);
        $scope.plugin.label = $scope.pluginCopy.label;
        MyNodeConfigService.setIsPluginBeingEdited(false);
        MyNodeConfigService.notifyPluginSaveListeners($scope.plugin.id);
      }
    };

    function validateSchema() {
      $scope.errors = [];
      var schema;
      try {
        schema = JSON.parse($scope.pluginCopy.outputSchema);
        schema = schema.fields;
      } catch (e) {
        schema = null;
      }

      var validationRules = [
        hasUniqueFields
      ];

      var error = [];
      validationRules.forEach(function (rule) {
        rule.call(this, schema, error);
      });

      if (error.length > 0) {
        $scope.errors = error;
        return false;
      } else {
        return true;
      }

    }

    function hasUniqueFields(schema, error) {
      if (!schema) { return true; }

      var fields = schema.map(function (field) { return field.name; });
      var unique = _.uniq(fields);

      if (fields.length !== unique.length) {
        error.push('There are two or more fields with the same name.');
      }
    }

  });
