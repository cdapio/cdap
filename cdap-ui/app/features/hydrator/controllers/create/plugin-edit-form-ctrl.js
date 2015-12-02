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

angular.module(PKG.name + '.feature.hydrator')
  .controller('PluginEditController', function($scope, PluginConfigFactory, myHelpers, EventPipe, $timeout, GLOBALS) {

    this.configfetched = false;
    this.properties = [];
    this.noconfig = null;

    this.noproperty = Object.keys(
      $scope.plugin._backendProperties || {}
    ).length;

    if (this.noproperty) {
      PluginConfigFactory.fetch(
        $scope.type,
        $scope.plugin.name
      )
        .then(
          (res) => {

            this.groupsConfig = PluginConfigFactory.generateNodeConfig($scope.plugin._backendProperties, res);
            this.groups = this.groupsConfig.groups;
            angular.forEach(this.groupsConfig.groups, (group) => {
              angular.forEach(group.fields, (field, name) => {
                if (field.defaultValue) {
                  $scope.plugin.properties[name] = $scope.plugin.properties[name] || field.defaultValue;
                }
              });
            });
            if (this.groupsConfig.outputSchema.implicitSchema) {
              var keys = Object.keys(res.outputschema.implicit);
              var formattedSchema = [];
              angular.forEach(keys, function (key) {
                formattedSchema.push({
                  name: key,
                  type: res.outputschema.implicit[key]
                });
              });

              $scope.plugin.outputSchema = JSON.stringify({ fields: formattedSchema });
            }

            if (this.groupsConfig.isOutputSchemaExists && !this.groupsConfig.outputSchema.implicitSchema) {
              if ($scope.plugin.properties[this.groupsConfig.outputSchemaProperty[0]] !== $scope.plugin.outputSchema) {
                $scope.plugin.properties[this.groupsConfig.outputSchemaProperty[0]] = $scope.plugin.outputSchema;
              }
              $scope.$watch('plugin.outputSchema', function() {
                if(validateSchema()) {
                  $scope.plugin.properties[this.groupsConfig.outputSchemaProperty[0]] = $scope.plugin.outputSchema;
                }
              });
            }

            // Mark the configfetched to show that configurations have been received.
            this.configfetched = true;
            this.config = res;
            this.noconfig = false;

          },
          (err) => {
            var propertiesFromBackend = Object.keys($scope.plugin._backendProperties);
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
            propertiesFromBackend.forEach(function (property) {
              $scope.plugin.properties[property] = $scope.plugin.properties[property] || '';
            });
          }
        );
    } else {
      this.configfetched = true;
    }

    this.schemaClear = function () {
      EventPipe.emit('schema.clear');
    };

    function validateSchema() {
      $scope.errors = [];
      var schema;
      try {
        schema = JSON.parse($scope.plugin.outputSchema);
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
