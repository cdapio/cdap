/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

class HydratorPlusPlusNodeConfigCtrl {
  constructor($scope, $timeout, $state, HydratorPlusPlusPluginConfigFactory, EventPipe, GLOBALS, HydratorPlusPlusConfigActions, myHelpers, NonStorePipelineErrorFactory, $uibModal, HydratorPlusPlusConfigStore, rPlugin, rDisabled, HydratorPlusPlusHydratorService, myPipelineApi, HydratorPlusPlusPreviewStore, rIsStudioMode) {
    'ngInject';
    this.$scope = $scope;
    this.$timeout = $timeout;
    this.$state = $state;
    this.EventPipe = EventPipe;
    this.HydratorPlusPlusPluginConfigFactory = HydratorPlusPlusPluginConfigFactory;
    this.GLOBALS = GLOBALS;
    this.myHelpers = myHelpers;
    this.HydratorPlusPlusConfigActions = HydratorPlusPlusConfigActions;
    this.NonStorePipelineErrorFactory = NonStorePipelineErrorFactory;
    this.requiredPropertyError = this.GLOBALS.en.hydrator.studio.error['GENERIC-MISSING-REQUIRED-FIELDS'];
    this.showPropagateConfirm = false; // confirmation dialog in node config for schema propagation.
    this.$uibModal = $uibModal;
    this.ConfigStore = HydratorPlusPlusConfigStore;
    this.$scope.isDisabled = rDisabled;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.myPipelineApi = myPipelineApi;
    this.previewStore = HydratorPlusPlusPreviewStore;

    this.setDefaults(rPlugin);
    this.tabs = [
      {
        label: 'Configuration',
        templateUrl: '/assets/features/hydrator/templates/partial/node-config-modal/configuration-tab.html'
      },
      {
        label: 'Preview',
        templateUrl: '/assets/features/hydrator/templates/partial/node-config-modal/preview-tab.html'
      },
      {
        label: 'Reference',
        templateUrl: '/assets/features/hydrator/templates/partial/node-config-modal/reference-tab.html'
      }
    ];

    this.showContents();

    this.isStudioMode = rIsStudioMode;
    this.isPreviewMode = this.previewStore.getState().preview.isPreviewModeEnabled;

    if (rIsStudioMode && this.isPreviewMode) {
      this.previewLoading = false;
      this.previewData = null;
      this.fetchPreview();
    }

    this.activeTab = this.isPreviewMode ? 2 : 1;


    // Timeouts
    this.setStateTimeout = null;
    this.importSchemaTimeout = null;
    this.$scope.$on('$destroy', () => {
      this.$timeout.cancel(this.setStateTimeout);
      this.$timeout.cancel(this.importSchemaTimeout);
    });

  }
  showContents() {
    if (angular.isArray(this.state.watchers)) {
      this.state.watchers.forEach(watcher => watcher());
      this.state.watchers = [];
    }
    if (Object.keys(this.state.node).length) {
      this.configfetched = false;

      this.$timeout.cancel(this.setStateTimeout);
      this.setStateTimeout = this.$timeout(() => {
        this.loadNewPlugin();
        this.validateNodeLabel();
      });
    }
  }
  validateNodeLabel() {
    let nodes = this.ConfigStore.getNodes();
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

      windowMode: 'regular',

      isValidPlugin: config.isValidPlugin || false,
      node: angular.copy(config.node) || {},

      isSource: config.isSource || false,
      isSink: config.isSink || false,
      isTransform: config.isTransform || false,
      isAction: config.isAction || false,

      type: config.appType || null,
      watchers: [],
      outputSchemaUpdate: 0
    };
    this.showPropagateConfirm = false;
  }
  propagateSchemaDownStream() {
    this.HydratorPlusPlusConfigActions.propagateSchemaDownStream(this.state.node.name);
  }
  loadNewPlugin() {
    const noJsonErrorHandler = (err) => {
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
          'HydratorPlusPlusNodeConfigCtrl.state.node',
          () => {
            this.validateNodeLabel(this);
            this.HydratorPlusPlusConfigActions.editPlugin(this.state.node.name, this.state.node);
          },
          true
        )
      );
    };

    this.state.noproperty = Object.keys(
      this.state.node._backendProperties || {}
    ).length;
    if (this.state.noproperty) {
      var artifactName = this.myHelpers.objectQuery(this.state.node, 'plugin', 'artifact', 'name');
      var artifactVersion = this.myHelpers.objectQuery(this.state.node, 'plugin', 'artifact', 'version');
      var artifactScope = this.myHelpers.objectQuery(this.state.node, 'plugin', 'artifact', 'scope');
      this.HydratorPlusPlusPluginConfigFactory.fetchWidgetJson(
        artifactName,
        artifactVersion,
        artifactScope,
        `widgets.${this.state.node.plugin.name}-${this.state.node.type}`
      )
        .then(
          (res) => {
            try {
              this.state.groupsConfig = this.HydratorPlusPlusPluginConfigFactory
                .generateNodeConfig(this.state.node._backendProperties, res);
            } catch(e) {
              noJsonErrorHandler();
              return;
            }
            const generateJumpConfig = (jumpConfig, properties) => {
              let streams = [], datasets = [];
              let jumpConfigStreams = jumpConfig.streams || [],
                  jumpConfigDatasets = jumpConfig.datasets || [];
              streams = jumpConfigStreams.map(stream => ({ streamId: properties[stream['ref-property-name']] , entityType: 'streams' }));
              datasets = jumpConfigDatasets.map(dataset => ({ datasetId: properties[dataset['ref-property-name']], entityType: 'datasets' }));
              return {streams, datasets};
            };
            if (res.errorDataset || this.state.node.errorDatasetName) {
              this.state.showErrorDataset = true;
              this.state.errorDatasetTooltip = res.errorDataset && res.errorDataset.errorDatasetTooltip || false;
              this.state.node.errorDatasetName = this.state.node.errorDatasetName || '';
            }

            if (this.$scope.isDisabled && this.state.groupsConfig.jumpConfig && Object.keys(this.state.groupsConfig.jumpConfig).length) {
              let {streams, datasets} = generateJumpConfig(this.state.groupsConfig.jumpConfig, this.state.node.plugin.properties);
              this.state.groupsConfig.jumpConfig.datasets = streams.concat(datasets);
            } else {
              // If we isDisabled is set to false then we are in studio mode & hence remove jump config.
              // Jumpconfig is only for published view where everything is disabled.
              delete this.state.groupsConfig.jumpConfig;
            }
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
              this.state.node.outputSchema = this.HydratorPlusPlusHydratorService.formatOutputSchemaToAvro(configOutputSchema.implicitSchema);
              this.HydratorPlusPlusConfigActions.editPlugin(this.state.node.name, this.state.node);
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
                  this.$scope.$watch('HydratorPlusPlusNodeConfigCtrl.state.node.outputSchema', () => {
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
                  'HydratorPlusPlusNodeConfigCtrl.state.node',
                  () => {
                    this.validateNodeLabel(this);
                    this.HydratorPlusPlusConfigActions.editPlugin(this.state.node.name, this.state.node);
                  },
                  true
                )
              );
            }
            if (!this.state.node.outputSchema) {
              this.state.node.outputSchema = this.myHelpers.objectQuery(this.state.node, 'inputSchema', 0, 'schema') || '';
            }
            if (!this.state.node.plugin.label) {
              this.state.node.plugin.label = this.state.node.name;
            }
            // Mark the configfetched to show that configurations have been received.
            this.state.configfetched = true;
            this.state.config = res;
            this.state.noconfig = false;
          },
          noJsonErrorHandler
        );
    } else {
      this.state.configfetched = true;
    }
  }
  schemaClear() {
    this.EventPipe.emit('schema.clear');
  }
  importSchema() {
    this.$timeout.cancel(this.importSchemaTimeout);
    this.importSchemaTimeout = this.$timeout(() => {
      document.getElementById('schema-import-link').click();
    });
  }
  importFiles(files) {
    let reader = new FileReader();
    reader.readAsText(files[0], 'UTF-8');

    reader.onload = (evt) => {
      let data = evt.target.result;
      this.EventPipe.emit('schema.import', data);
    };
  }
  exportSchema() {
    this.EventPipe.emit('schema.export');
  }

  toggleMaximizedView(isExpanded) {
    this.state.windowMode = (isExpanded) ? 'expand' : 'regular';
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

  // PREVIEW
  fetchPreview() {
    this.previewLoading = true;
    let previewId = this.previewStore.getState().preview.previewId;

    if (!previewId) {
      this.previewLoading = false;
      return;
    }
    let params = {
      namespace: this.$state.params.namespace,
      previewId: previewId,
      stage: this.state.node.plugin.label,
      scope: this.$scope
    };
    this.myPipelineApi.getStagePreview(params)
      .$promise
      .then((res) => {
        this.previewData = {
          input: {},
          output: {},
          numInputStages: 0
        };

        if (!this.state.isSource) {
          this.previewData.input = this.formatMultipleRecords(res['input.records']);
          this.previewData.numInputStages = Object.keys(this.previewData.input).length;
        }
        if (!this.state.isSink) {
          this.previewData.output = this.formatRecords(res['output.records']);
        }
        this.previewLoading = false;
      }, () => {
        this.previewLoading = false;
      });
  }

  formatMultipleRecords(records) {
    let mapInputs = {};

    angular.forEach(records, (record) => {
      let json = {};
      try {
        json = JSON.parse(record);
      } catch (e) {
        console.log('ERROR', e);
        return;
      }

      if (!json.value.schema) { return; }

      let schema = json.value.schema.fields.map( (field) => {
        return field.name;
      });

      let data = json.value.fields;

      if (!mapInputs[json.key]) {
        mapInputs[json.key] = {
          schema: schema,
          records: []
        };
      }

      mapInputs[json.key].records.push(data);
    });

    return mapInputs;
  }

  formatRecords(records) {
    if (!records) {
      return {
        schema: [],
        records: []
      };
    }

    let jsonRecords = records.map( (record) => {
      let json = {};
      try {
        json = JSON.parse(record);
      } catch (e) {
        console.log('ERROR', e);
        return json;
      }
      return json;
    });

    let schema = jsonRecords[0].value.schema.fields.map( (field) => {
      return field.name;
    });

    let data = jsonRecords.map( (record) => {
      return record.value.fields;
    });

    return {
      schema: schema,
      records: data
    };
  }


  // Wrangler
  openWranglerModal() {
    this.$uibModal.open({
      controller: 'WranglerModalController',
      controllerAs: 'Wrangler',
      windowClass: 'wrangler-modal',
      templateUrl: '/assets/features/hydrator/templates/create/Wrangler/wrangler-modal.html',
      resolve: {
        rPlugin: () => {
          return this.state.node;
        }
      }
    });
  }

}

angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorPlusPlusNodeConfigCtrl', HydratorPlusPlusNodeConfigCtrl);
