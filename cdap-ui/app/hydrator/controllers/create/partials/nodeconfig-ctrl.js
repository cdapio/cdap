/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
  constructor($scope, $timeout, $state, HydratorPlusPlusPluginConfigFactory, EventPipe, GLOBALS, HydratorPlusPlusConfigActions, myHelpers, NonStorePipelineErrorFactory, $uibModal, HydratorPlusPlusConfigStore, rPlugin, rDisabled, HydratorPlusPlusHydratorService, myPipelineApi, HydratorPlusPlusPreviewStore, rIsStudioMode, HydratorPlusPlusOrderingFactory, avsc, LogViewerStore, DAGPlusPlusNodesActionsFactory, rNodeMetricsContext, HydratorPlusPlusDetailMetricsStore, HydratorPlusPlusNodeService) {
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
    this.HydratorPlusPlusOrderingFactory = HydratorPlusPlusOrderingFactory;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;
    this.avsc = avsc;
    this.LogViewerStore = LogViewerStore;
    this.HydratorPlusPlusDetailMetricsStore = HydratorPlusPlusDetailMetricsStore;
    this.HydratorPlusPlusNodeService = HydratorPlusPlusNodeService;
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
      },
      {
        label: 'Metrics',
        templateUrl: '/assets/features/hydrator/templates/partial/node-config-modal/metrics-tab.html'
      }
    ];

    this.metricsContext = rNodeMetricsContext;
    this.isMetricsEnabled = this.$scope.isDisabled && (Array.isArray(rNodeMetricsContext.runs) && rNodeMetricsContext.runs.length);
    if (this.metricsContext) {
      this.nodeMetrics = [
        `user.${this.state.node.name}.records.in`,
        `user.${this.state.node.name}.records.error`,
        `user.${this.state.node.name}.process.time.total`,
        `user.${this.state.node.name}.process.time.avg`,
        `user.${this.state.node.name}.process.time.max`,
        `user.${this.state.node.name}.process.time.min`,
        `user.${this.state.node.name}.process.time.stddev`
      ];
      let nodeType = this.state.node.type || this.state.node.plugin.type;
      if (nodeType === 'splittertransform') {
        if (this.state.node.outputSchema && Array.isArray(this.state.node.outputSchema))   {
          angular.forEach(this.state.node.outputSchema, (port) => {
            this.nodeMetrics.push(`user.${this.state.node.name}.records.out.${port.name}`);
          });
        }
      } else {
        this.nodeMetrics.push(`user.${this.state.node.name}.records.out`);
      }
    } else {
      this.nodeMetrics = [];
    }
    this.showContents();

    this.isStudioMode = rIsStudioMode;
    this.isPreviewMode = this.previewStore.getState().preview.isPreviewModeEnabled;

    if (rIsStudioMode && this.isPreviewMode) {
      this.previewLoading = false;
      this.previewData = null;
      this.previewStatus = null;
      this.fetchPreview();
    }

    this.activeTab = 1;
    if (this.isPreviewMode && !rPlugin.isAction) {
      this.activeTab = 2;
    } else if (this.HydratorPlusPlusDetailMetricsStore.state.metricsTabActive) {
      this.activeTab = 4;
    }

    this.portMetricsToShow = this.HydratorPlusPlusDetailMetricsStore.state.portsToShow;

    this.$scope.$on('modal.closing', () => {
      this.updateNodeStateIfDirty();
    });

    // Timeouts
    this.setStateTimeout = null;
    this.$scope.$on('$destroy', () => {
      this.$timeout.cancel(this.setStateTimeout);
      this.EventPipe.cancelEvent('dataset.selected');
    });

    let vm = this;

    this.EventPipe.on('dataset.selected', (schema, format, datasetAlreadyExists, datasetId) => {
      if (datasetAlreadyExists) {
        vm.datasetAlreadyExists = datasetAlreadyExists;
      } else {
        vm.datasetAlreadyExists = false;
      }

      // if this plugin is having an existing dataset with a macro, then don't change anything.
      // else if the user is changing to another existing dataset, then show basic mode.
      if (this.myHelpers.objectQuery(vm, 'defaultState', 'node', 'plugin', 'properties', 'name') && vm.defaultState.node.plugin.properties.name !== datasetId) {
        vm.state.schemaAdvance = false;
      }
      if (datasetId) {
        vm.datasetId = datasetId;
      }
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
      isCondition: config.isCondition || false,

      type: config.appType || null,
      watchers: [],
      outputSchemaUpdate: 0,
      schemaAdvance: false
    };

    this.defaultState = angular.copy(this.state);

    let propertiesSchema = this.myHelpers.objectQuery(this.state.node, 'plugin', 'properties', 'schema');
    let schemaArr = propertiesSchema || this.state.node.outputSchema;

    if (schemaArr) {
      if (Array.isArray(schemaArr)) {
        angular.forEach(schemaArr, (schemaObj) => {
          if (schemaObj.schema) {
            try {
              this.avsc.parse(schemaObj.schema);
            } catch (e) {
              this.state.schemaAdvance = true;
            }
          }
        });
      } else {
        try {
          this.avsc.parse(schemaArr);
        } catch (e) {
          this.state.schemaAdvance = true;
        }
      }
    }

    this.showPropagateConfirm = false;
  }
  propagateSchemaDownStream() {
    this.HydratorPlusPlusConfigActions.propagateSchemaDownStream(this.state.node.name);
  }
  loadNewPlugin() {
    const noJsonErrorHandler = (err) => {
      var propertiesFromBackend = Object.keys(this.state.node._backendProperties);
      // Didn't receive a configuration from the backend. Fallback to all textboxes.
      switch (err) {
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
      this.defaultState = angular.copy(this.state);
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
        `widgets.${this.state.node.plugin.name}-${this.state.node.type || this.state.node.plugin.type}`
      )
        .then(
          (res) => {
            try {
              this.state.groupsConfig = this.HydratorPlusPlusPluginConfigFactory.generateNodeConfig(this.state.node._backendProperties, res);
            } catch (e) {
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
                field.errorTooltip = '\'' + field.label + '\' is a required field';
                if (field.defaultValue) {
                  this.state.node.plugin.properties[field.name] = this.state.node.plugin.properties[field.name] || field['widget-attributes'].default;
                }
              });
            });
            let listOfFields = _.flatten(this.state.groupsConfig.groups.map(group => group.fields));
            this.emptyHiddenFields = listOfFields
              .filter(field => {
                var defaultValue = this.myHelpers.objectQuery(field, 'widget-attributes', 'default');
                var widgetType = field['widget-type'];
                var isEmpty = function isEmpty(v) {
                  return _.isUndefined(v) || _.isNull(v) || _.isEmpty(v);
                };
                let requiredFields = _.values(this.state.node._backendProperties, function (field) {
                    return field;
                  }).filter(function (field) {
                    return field.required;
                  }).map(function (field) {
                    return field.name;
                  });
                return requiredFields.indexOf(field.name) !== -1 && widgetType === 'hidden' && isEmpty(defaultValue);
              })
              .map(field => '"' + field.name + '"');

            var configOutputSchema = this.state.groupsConfig.outputSchema;
            // If its an implicit schema, set the output schema to the implicit schema and inform ConfigActionFactory
            if (configOutputSchema.implicitSchema) {
              this.state.node.outputSchema = [this.HydratorPlusPlusNodeService.getOutputSchemaObj(this.HydratorPlusPlusHydratorService.formatSchemaToAvro(configOutputSchema.implicitSchema))];
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
                  this.state.node.plugin.properties[configOutputSchema.outputSchemaProperty[0]] = this.state.node.outputSchema[0].schema;
                }
                this.state.watchers.push(
                  this.$scope.$watch('HydratorPlusPlusNodeConfigCtrl.state.node.outputSchema', () => {
                    if (this.validateSchema()) {
                      this.state.node.plugin.properties[configOutputSchema.outputSchemaProperty[0]] = this.state.node.outputSchema[0].schema;
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
            if (!this.state.node.outputSchema || this.state.node.type === 'condition') {
              let inputSchema = this.myHelpers.objectQuery(this.state.node, 'inputSchema', 0, 'schema') || '';
              if (typeof inputSchema !== 'string') {
                inputSchema = JSON.stringify(inputSchema);
              }
              this.state.node.outputSchema = [this.HydratorPlusPlusNodeService.getOutputSchemaObj(inputSchema)];
            }
            if (!this.state.node.plugin.label) {
              this.state.node.plugin.label = this.state.node.name;
            }
            // Mark the configfetched to show that configurations have been received.
            this.state.configfetched = true;
            this.state.config = res;
            this.state.noconfig = false;
            this.defaultState = angular.copy(this.state);
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

    if (!Array.isArray(this.state.node.outputSchema)) {
      this.state.node.outputSchema = [this.HydratorPlusPlusNodeService.getOutputSchemaObj(this.state.node.outputSchema)];
    }

    angular.forEach(this.state.node.outputSchema, (schemaObj) => {
      let schema;
      try {
        schema = JSON.parse(schemaObj.schema);
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
        this.state.errors.push(error);
      }
    });

    if (this.state.errors.length) {
      return false;
    }
    return true;
  }
  hasUniqueFields(schema, error) {
    if (!schema) { return true; }

    var fields = schema.map(function (field) { return field.name; });
    var unique = _.uniq(fields);

    if (fields.length !== unique.length) {
      error.push('There are two or more fields with the same name.');
    }
  }
  updateNodeStateIfDirty() {
    let stateIsDirty = this.stateIsDirty();
    // because we are adding state to history before we open a node config, so if the config wasn't changed at all,
    // then we should remove that state from history
    if (!stateIsDirty) {
      this.DAGPlusPlusNodesActionsFactory.removePreviousState();
    // if it was changed, then reset future states so user can't redo
    } else {
      this.DAGPlusPlusNodesActionsFactory.resetFutureStates();
    }
  }
  stateIsDirty() {
    let defaults = this.defaultState.node;
    let state = this.state.node;
    return !angular.equals(defaults, state);
  }
  updateDefaultOutputSchema(outputSchema) {
    if (typeof outputSchema !== 'string') {
      outputSchema = JSON.stringify(outputSchema);
    }
    let configOutputSchema = this.state.groupsConfig.outputSchema;
    if (!configOutputSchema.implicitSchema && configOutputSchema.isOutputSchemaExists) {
      this.defaultState.node.outputSchema = outputSchema;
      this.defaultState.node.plugin.properties[configOutputSchema.outputSchemaProperty[0]] = this.defaultState.node.outputSchema;
    }
  }

  // PREVIEW
  fetchPreview() {
    this.previewLoading = true;
    let previewId = this.previewStore.getState().preview.previewId;
    let previousStageIsCondition = false;

    if (!previewId) {
      this.previewLoading = false;
      return;
    }
    let params = {
      namespace: this.$state.params.namespace,
      previewId: previewId,
      scope: this.$scope
    };

    let { stages, connections } = this.ConfigStore.getConfigForExport().config;
    let adjacencyMap = this.HydratorPlusPlusOrderingFactory.getAdjacencyMap(connections);
    let postBody = [];
    let previousStageName = Object.keys(adjacencyMap).find(key => adjacencyMap[key].indexOf(this.state.node.plugin.label) !== -1);

    if (previousStageName) {
      let previousStage = stages.find(stage => stage.name === previousStageName);

      // In case we have multiple condition nodes in a row, we have to keep traversing back
      // until we find a node that actually has records out
      while (previousStage && previousStage.plugin.type === 'condition') {
        previousStageIsCondition = true;
        previousStageName = Object.keys(adjacencyMap).find(key => adjacencyMap[key].indexOf(previousStageName) !== -1);
        previousStage = stages.find(stage => stage.name === previousStageName);
      }
      postBody.push(previousStageName);
    }

    this.myPipelineApi.getStagePreview(params, {
      tracers: postBody.concat([this.state.node.plugin.label])
    })
      .$promise
      .then((res) => {
        this.previewData = {
          input: {},
          output: {},
          numInputStages: 0
        };
        let recordsOut = {};
        let recordsIn = {};

        // Backend returns metrics for the stages listed in the `tracers` property in the API call,
        // usually that's the stage the user is clicking on and the last stage connecting to it that
        // has output records
        angular.forEach(res, (stageMetrics, stageName) => {
          if (stageName === this.state.node.plugin.label) {
            recordsOut = stageMetrics['records.out'];
          } else { // set the records in of current stage to records out of previous stage with data
            recordsIn[stageName] = this.formatMultipleRecords(stageMetrics['records.out']);
          }
        });

        if (!this.state.isSink) {
          this.previewData.output = this.formatMultipleRecords(recordsOut);
        }
        if (!this.state.isSource) {
          this.previewData.input = recordsIn;
          this.previewData.numInputStages = Object.keys(res).length;
        }

        // set current stage's input records to null only if its output records are null AND if the previous
        // stage is a Condition, since that means data didn't flow through to that branch
        if (_.isEmpty(recordsOut) && previousStageIsCondition) {
          this.previewData.input = {};
          this.previewData.numInputStages = 0;
        }

        let logViewerState = this.LogViewerStore.getState();
        if (logViewerState.statusInfo) {
          // TODO: Move preview status state info HydratorPlusPlusPreviewStore, then get from there
          this.previewStatus = logViewerState.statusInfo.status;
        }
        this.previewLoading = false;
      }, () => {
        this.previewLoading = false;
      });
  }

  formatMultipleRecords(records) {
    let mapInputs = {
      schema: {},
      records: []
    };

    angular.forEach(records, (record) => {
      let json = record;
      if (json.value) {
        json = json.value;
      }

      if (!json.schema) { return; }

      let schema = json.schema.fields.map( (field) => {
        return field.name;
      });

      let data = json.fields;

      mapInputs.schema = schema;
      mapInputs.records.push(data);
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

    let jsonRecords = records;

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

  // MACRO ENABLED SCHEMA
  toggleAdvance() {
    if (this.state.node.outputSchema.length > 0) {
      try {
        this.avsc.parse(this.state.node.outputSchema[0].schema);
      } catch (e) {
        this.state.node.outputSchema = [this.HydratorPlusPlusNodeService.getOutputSchemaObj('')];
      }
    }

    this.state.schemaAdvance = !this.state.schemaAdvance;
  }
}

angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorPlusPlusNodeConfigCtrl', HydratorPlusPlusNodeConfigCtrl);
