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

class NodeConfigController {
  constructor(NodeConfigStore, $scope, $timeout, $state, DetailNonRunsStore, PluginConfigFactory, EventPipe, GLOBALS, ConfigActionsFactory) {

    this.$scope = $scope;
    this.$timeout = $timeout;
    this.$state = $state;
    this.EventPipe = EventPipe;
    this.DetailNonRunsStore = DetailNonRunsStore;
    this.PluginConfigFactory = PluginConfigFactory;
    this.GLOBALS = GLOBALS;
    this.NodeConfigStore = NodeConfigStore;
    this.ConfigActionsFactory = ConfigActionsFactory;

    this.setDefaults();
    NodeConfigStore.registerOnChangeListener(this.setState.bind(this));
  }
  setState() {
    var appType = this.$state.params.type || this.DetailNonRunsStore.getAppType();
    var nodeState = this.NodeConfigStore.getState();
    this.state = {
      configfetched : false,
      properties : [],
      noconfig: null,
      noproperty: true,
      config: {},

      isValidPlugin: nodeState.isValidPlugin,
      plugin: nodeState.plugin,

      isSource: nodeState.isSource,
      isSink: nodeState.isSink,
      isTransform: nodeState.isTransform,

      type: appType
    };
    this.loadNewPlugin();
  }
  setDefaults() {
    this.state = {
      configfetched : false,
      properties : [],
      noconfig: null,
      noproperty: true,
      config: {},

      isValidPlugin: false,
      plugin: {},

      isSource: false,
      isSink: false,
      isTransform: false,

      type: null
    };
  }
  loadNewPlugin() {

    this.state.noproperty = Object.keys(
      this.state.plugin._backendProperties || {}
    ).length;

    if (this.state.noproperty) {
      this.PluginConfigFactory.fetch(
        this.state.type,
        this.state.plugin.name
      )
        .then(
          (res) => {

            this.state.groupsConfig = this.PluginConfigFactory.generateNodeConfig(this.state.plugin._backendProperties, res);
            angular.forEach(this.state.groupsConfig.groups, (group) => {
              angular.forEach(group.fields, (field, name) => {
                if (field.defaultValue) {
                  this.state.plugin.properties[name] = this.state.plugin.properties[name] || field.defaultValue;
                }
              });
            });
            if (this.state.groupsConfig.outputSchema.implicitSchema) {
              var keys = Object.keys(res.outputschema.implicit);
              var formattedSchema = [];
              angular.forEach(keys, function (key) {
                formattedSchema.push({
                  name: key,
                  type: res.outputschema.implicit[key]
                });
              });

              this.state.plugin.outputSchema = JSON.stringify({ fields: formattedSchema });
            }

            if (this.state.groupsConfig.isOutputSchemaExists && !this.state.groupsConfig.outputSchema.implicitSchema) {
              if (this.state.plugin.properties[this.state.groupsConfig.outputSchemaProperty[0]] !== this.state.outputSchema) {
                this.state.properties[this.state.groupsConfig.outputSchemaProperty[0]] = this.state.outputSchema;
              }
              if (!this.$scope.isDisabled) {
                this.$scope.$watch('plugin.outputSchema', function() {
                  if(this.validateSchema()) {
                    this.state.properties[this.state.groupsConfig.outputSchemaProperty[0]] = this.state.outputSchema;
                  }
                });
              }
            }
            if (!this.$scope.isDisabled) {
              this.$scope.$watch(
                'NodeConfigController.state.plugin.properties',
                _.debounce(() => this.ConfigActionsFactory.editPlugin(this.state.plugin.id, this.state.plugin.properties), 1000),
                true
              );
            }

            // Mark the configfetched to show that configurations have been received.
            this.state.configfetched = true;
            this.state.config = res;
            this.state.noconfig = false;

          },
          (err) => {
            var propertiesFromBackend = Object.keys(this.state._backendProperties);
            // Didn't receive a configuration from the backend. Fallback to all textboxes.
            switch(err) {
              case 'NO_JSON_FOUND':
                this.state.noConfigMessage = this.GLOBALS.en.hydrator.studio.noConfigMessage;
                break;
              case 'CONFIG_SYNTAX_JSON_ERROR':
                this.state.noConfigMessage = this.GLOBALS.en.hydrator.studio.syntaxConfigJsonError;
                break;
              case 'CONFIG_SEMANTICS_JSON_ERROR':
                this.state.noConfigMessage = this.GLOBALS.en.hydrator.studio.semanticConfigJsonError;
                break;
            }
            this.state.noconfig = true;
            this.state.configfetched = true;
            propertiesFromBackend.forEach(function (property) {
              this.state.plugin.properties[property] = this.state.plugin.properties[property] || '';
            });
          }
        );
    } else {
      this.state.configfetched = true;
    }
  }
  schemaClear() {
    this.EventPipe.emit('schema.clear');
  }
  validateSchema() {
    this.state.errors = [];
    var schema;
    try {
      schema = JSON.parse(this.state.plugin.outputSchema);
      schema = schema.fields;
    } catch (e) {
      schema = null;
    }

    var validationRules = [
      this.hasUniqueFields
    ];

    var error = [];
    validationRules.forEach(function (rule) {
      rule.call(this, schema, error);
    });

    if (error.length > 0) {
      this.state.errors = error;
      return false;
    } else {
      return true;
    }
  }
  hasUniqueFields(schema, error) {
    if (!schema) { return true; }

    var fields = schema.map(function (field) { return field.name; });
    var unique = _.uniq(fields);

    if (fields.length !== unique.length) {
      error.push('There are two or more fields with the same name.');
    }
  }
}

NodeConfigController.$inject = ['NodeConfigStore', '$scope', '$timeout', '$state', 'DetailNonRunsStore', 'PluginConfigFactory', 'EventPipe', 'GLOBALS', 'ConfigActionsFactory'];

angular.module(PKG.name + '.feature.hydrator')
  .controller('NodeConfigController', NodeConfigController);
