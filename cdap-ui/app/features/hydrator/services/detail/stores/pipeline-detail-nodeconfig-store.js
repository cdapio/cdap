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
  .service('NodeConfigStore', function(PipelineNodeConfigDispatcher, $q, $filter, IMPLICIT_SCHEMA, GLOBALS, myPipelineApi, $state, $rootScope, ConfigStore, DetailNonRunsStore, ConfigActionsFactory, HydratorService) {

    var dispatcher;
    this.changeListeners = [];
    this.setDefaults = function() {
      this.state = {
        node: {},
        isValidPlugin: false,
        isSource: false,
        isSink: false,
        isTransform: false
      };
    };
    this.setDefaults();
    this.HydratorService = HydratorService;
    this.getState = function() {
      return this.state;
    };

    this.setState = function(plugin) {
      onPluginChange.call(this, plugin);
    };
    this.setPlugin = function(plugin) {
      this.plugin = plugin;
      var type = this.isSource ? 'source': false;
      if (!type && this.isSink) {
        type = 'sink';
      } else {
        type = 'transform';
      }

      ConfigActionsFactory.savePlugin(plugin, type);
    };
    this.registerOnChangeListener = function(callback) {
      this.changeListeners.push(callback);
    };
    this.emitChange = function() {
      this.changeListeners.forEach(function(callback) {
        callback();
      });
    };

    this.reset = function() {
      // This is done here as NodeConfigStore is being reused between create and view pipelines states.
      // So we need to destroy the dispatcher updating all listeners of the store so when we switch states
      // one does not get notified if out of context.
      PipelineNodeConfigDispatcher.destroyDispatcher();
      this.changeListeners = [];
    };
    this.init = function() {
      if ($state.includes('hydrator.create.**')) {
        this.ConfigStore = ConfigStore;
      } else if ($state.includes('hydrator.detail.**')) {
        this.ConfigStore = DetailNonRunsStore;
      }
      dispatcher = PipelineNodeConfigDispatcher.getDispatcher();
      dispatcher.register('onPluginChange', this.setState.bind(this));
      dispatcher.register('onPluginRemove', function() {
        this.setDefaults();
        this.emitChange();
      }.bind(this));
      dispatcher.register('onReset', this.setDefaults.bind(this));
      dispatcher.register('onPluginSave', this.setPlugin.bind(this));
    };

    function onPluginChange(node) {
      if (node && this.state.node && node.name === this.state.node.name) {
        return;
      }
      switchPlugin.call(this, node)
        .then(this.emitChange.bind(this));
    }

    function switchPlugin(node) {
      // This is super wrong. While re-writing this in flux architecture this should go away.
      this.state.node = node;
      this.state.isValidPlugin = false;
      return setPluginInfo
              .call(this)
              .then(
                configurePluginInfo.bind(this),
                () => console.error('Fetching backend properties for :',this.state.plugin.name, ' failed.')
              );
    }

    function setPluginInfo() {
      this.state.isSource = false;
      this.state.isTransform = false;
      this.state.isSink = false;
      if (this.state.node._backendProperties) {
        return $q.when(true);
      } else {
        return this.HydratorService.fetchBackendProperties(this.state.node, this.ConfigStore.getAppType());
      }
    }

    function configurePluginInfo(pluginDetails) {
      var pluginId = this.state.node.name;
      var input;
      var sourceConn = $filter('filter')(this.ConfigStore.getConnections(), { to: pluginId });
      var sourceSchema = null;
      var isStreamSource = false;

      var clfSchema = IMPLICIT_SCHEMA.clf;

      var syslogSchema = IMPLICIT_SCHEMA.syslog;

      var source;
      if (sourceConn && sourceConn.length) {
        source = this.ConfigStore.getNode(sourceConn[0].from);
        sourceSchema = source.outputSchema;

        if (source.name === 'Stream') {
          isStreamSource = true;
        }

        if (source.plugin.properties.format && source.plugin.properties.format === 'clf') {
          sourceSchema = clfSchema;
        } else if (source.plugin.properties.format && source.plugin.properties.format === 'syslog') {
          sourceSchema = syslogSchema;
        }

      } else {
        sourceSchema = '';
      }

      try {
        input = JSON.parse(sourceSchema);
      } catch (e) {
        input = null;
      }

      if (isStreamSource) {
        // Must be in this order!!
        if (!input) {
          input = {
            fields: [{ name: 'body', type: 'string' }]
          };
        }

        input.fields.unshift({
          name: 'headers',
          type: {
            type: 'map',
            keys: 'string',
            values: 'string'
          }
        });

        input.fields.unshift({
          name: 'ts',
          type: 'long'
        });
      }

      this.state.node.inputSchema = input ? input.fields : null;
      angular.forEach(this.state.node.inputSchema, function (field) {
        if (angular.isArray(field.type)) {
          field.type = field.type[0];
          field.nullable = true;
        } else {
          field.nullable = false;
        }
      });

      if (!this.state.node.outputSchema && input) {
        this.state.node.outputSchema = JSON.stringify(input) || null;
      }

      this.state.node._backendProperties = pluginDetails._backendProperties || this.state.node._backendProperties;
      this.state.node.description = pluginDetails.description || this.state.node.description;
      this.state.isValidPlugin = Object.keys(this.state.node).length;

      var artifactTypeExtension = GLOBALS.pluginTypes[this.ConfigStore.getAppType()];
      if (this.state.node.type === artifactTypeExtension.source) {
        this.state.isSource = true;
      }

      if (this.state.node.type === artifactTypeExtension.sink) {
        this.state.isSink = true;
      }
      if (this.state.node.type === 'transform') {
        this.state.isTransform = true;
      }
    }

  });
