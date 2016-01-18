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
  constructor(NodeConfigStore, $scope, $timeout, $state, DetailNonRunsStore, PluginConfigFactory, EventPipe, GLOBALS, ConfigActionsFactory, myHelpers, NonStorePipelineErrorFactory) {

    this.$scope = $scope;
    this.$timeout = $timeout;
    this.$state = $state;
    this.EventPipe = EventPipe;
    this.DetailNonRunsStore = DetailNonRunsStore;
    this.PluginConfigFactory = PluginConfigFactory;
    this.GLOBALS = GLOBALS;
    this.myHelpers = myHelpers;
    this.NodeConfigStore = NodeConfigStore;
    this.ConfigActionsFactory = ConfigActionsFactory;
    this.NonStorePipelineErrorFactory = NonStorePipelineErrorFactory;
    this.requiredPropertyError = this.GLOBALS.en.hydrator.studio.error['GENERIC-MISSING-REQUIRED-FIELDS'];
    this.showPropagateConfirm = false; // confirmation dialog in node config for schema propagation.
    this.$timeout = $timeout;

    this.setDefaults();
    NodeConfigStore.registerOnChangeListener(this.setState.bind(this));
  }
  setState() {
    var appType = this.$state.params.type || this.NodeConfigStore.ConfigStore.getAppType();
    var nodeState = this.NodeConfigStore.getState();
    nodeState.appType = appType;
    if (angular.isArray(this.state.watchers)) {
      this.state.watchers.forEach(watcher => watcher());
      this.state.watchers = [];
    }
    this.setDefaults(nodeState);
    if (Object.keys(nodeState.node).length) {
      this.configfetched = false;
      this.$timeout(() => {
        this.loadNewPlugin();
        this.validateNodeLabel();
      });
    }
  }
  validateNodeLabel() {
    let nodes = this.NodeConfigStore.ConfigStore.getNodes();
    let nodeName = this.myHelpers.objectQuery(this.state, 'node', 'plugin', 'label');
    if (!nodeName) {
      return;
    }
    this.NonStorePipelineErrorFactory.isNodeNameUnique(nodeName, nodes, err => {
      if (err) {
        this.state.nodeLabelError = this.GLOBALS.en.hydrator.studio.error[err];
      } else {
        this.state.nodeLabelError = '';
      }
    });
  }
  setDefaults(config = {}) {
    this.state = {
      configfetched : false,
      properties : [],
      noconfig: null,
      noproperty: true,
      config: {},
      groupsConfig: {},

      isValidPlugin: config.isValidPlugin || false,
      node: angular.copy(config.node) || {},

      isSource: config.isSource || false,
      isSink: config.isSink || false,
      isTransform: config.isTransform || false,

      type: config.appType || null,
      watchers: [],
      outputSchemaUpdate: 0
    };
  }
  propagateSchemaDownStream() {
    this.ConfigActionsFactory.propagateSchemaDownStream(this.state.node.name);
  }
  loadNewPlugin() {
    this.state.noproperty = Object.keys(
      this.state.node._backendProperties || {}
    ).length;

    if (this.state.noproperty) {
      var artifactName = this.myHelpers.objectQuery(this.state.node, 'plugin', 'artifact', 'name');
      var artifactVersion = this.myHelpers.objectQuery(this.state.node, 'plugin', 'artifact', 'version');
      this.PluginConfigFactory.fetchWidgetJson(
        artifactName,
        artifactVersion,
        `widgets.${this.state.node.plugin.name}-${this.state.node.type}`
      )
        .then(
          (res) => {
            this.state.groupsConfig = this.PluginConfigFactory.generateNodeConfig(this.state.node._backendProperties, res);
            angular.forEach(this.state.groupsConfig.groups, (group) => {
              angular.forEach(group.fields, (field) => {
                if (field.defaultValue) {
                  this.state.node.plugin.properties[field.name] = this.state.node.plugin.properties[field.name] || field.defaultValue;
                }
              });
            });
            var configOutputSchema = this.state.groupsConfig.outputSchema;
            // If its an implicit schema, set the output schema to the implicit schema and inform ConfigActionFactory
            if (configOutputSchema.implicitSchema) {
              var keys = Object.keys(configOutputSchema.implicitSchema);
              var formattedSchema = [];
              angular.forEach(keys, (key) => {
                formattedSchema.push({
                  name: key,
                  type: configOutputSchema.implicitSchema[key]
                });
              });
              this.state.node.outputSchema = JSON.stringify({ fields: formattedSchema });
              this.ConfigActionsFactory.editPlugin(this.state.node.name, this.state.node);
            } else {
              // If not an implcit schema check if a schema property exists in the node config.
              // What this means is, has the plugin developer specified a plugin property in 'outputs' array of node config.
              // If yes then set it as output schema and everytime when a user edits the output schema the value has to
              // be transitioned to the respective plugin property.
              if (configOutputSchema.isOutputSchemaExists) {
                let schemaProperty = configOutputSchema.outputSchemaProperty[0];
                let pluginProperties = this.state.node.plugin.properties;
                if (pluginProperties[schemaProperty]) {
                  this.state.node.outputSchema = pluginProperties[schemaProperty];
                } else if (pluginProperties[schemaProperty] !== this.state.node.outputSchema) {
                  this.state.node.plugin.properties[configOutputSchema.outputSchemaProperty[0]] = this.state.node.outputSchema;
                }
                this.state.watchers.push(
                  this.$scope.$watch('NodeConfigController.state.node.outputSchema', () => {
                    if(this.validateSchema()) {
                      this.state.node.plugin.properties[configOutputSchema.outputSchemaProperty[0]] = this.state.node.outputSchema;
                    }
                  })
                );
              }
            }
            if (!this.$scope.isDisabled) {
              this.state.watchers.push(
                this.$scope.$watch(
                  'NodeConfigController.state.node',
                  () => {
                    this.validateNodeLabel(this);
                    this.ConfigActionsFactory.editPlugin(this.state.node.name, this.state.node);
                  },
                  true
                )
              );
            }
            if (!this.state.node.outputSchema) {
              this.state.node.outputSchema = JSON.stringify({fields: this.state.node.inputSchema});
            }
            // Mark the configfetched to show that configurations have been received.
            this.state.configfetched = true;
            this.state.config = res;
            this.state.noconfig = false;
          },
          (err) => {
            var propertiesFromBackend = Object.keys(this.state.node._backendProperties);
            // Didn't receive a configuration from the backend. Fallback to all textboxes.
            switch(err) {
              case 'NO_JSON_FOUND':
                this.state.noConfigMessage = this.GLOBALS.en.hydrator.studio.info['NO-CONFIG'];
                break;
              case 'CONFIG_SYNTAX_JSON_ERROR':
                this.state.noConfigMessage = this.GLOBALS.en.hydrator.studio.error['SYNTAX-CONFIG-JSON'];
                break;
              case 'CONFIG_SEMANTICS_JSON_ERROR':
                this.state.noConfigMessage = this.GLOBALS.en.hydrator.studio.error['SEMANTIC-CONFIG-JSON'];
                break;
            }
            this.state.noconfig = true;
            this.state.configfetched = true;
            propertiesFromBackend.forEach( (property) => {
              this.state.node.plugin.properties[property] = this.state.node.plugin.properties[property] || '';
            });
            this.state.watchers.push(
              this.$scope.$watch(
                'NodeConfigController.state.node',
                () => {
                  this.validateNodeLabel(this);
                  this.ConfigActionsFactory.editPlugin(this.state.node.name, this.state.node);
                },
                true
              )
            );
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
      schema = JSON.parse(this.state.node.outputSchema);
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

NodeConfigController.$inject = ['NodeConfigStore', '$scope', '$timeout', '$state', 'DetailNonRunsStore', 'PluginConfigFactory', 'EventPipe', 'GLOBALS', 'ConfigActionsFactory', 'myHelpers', 'NonStorePipelineErrorFactory'];

angular.module(PKG.name + '.feature.hydrator')
  .controller('NodeConfigController', NodeConfigController);
