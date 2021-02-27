/*
 * Copyright © 2015-2019 Cask Data, Inc.
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
  constructor($scope,
    $timeout,
    $state,
    HydratorPlusPlusPluginConfigFactory,
    EventPipe,
    GLOBALS,
    HydratorPlusPlusConfigActions,
    myHelpers,
    NonStorePipelineErrorFactory,
    $uibModal,
    HydratorPlusPlusConfigStore,
    rPlugin,
    rDisabled,
    HydratorPlusPlusHydratorService,
    myPipelineApi,
    HydratorPlusPlusPreviewStore,
    rIsStudioMode,
    HydratorPlusPlusOrderingFactory,
    avsc,
    DAGPlusPlusNodesActionsFactory,
    rNodeMetricsContext,
    HydratorPlusPlusNodeService,
    HydratorPlusPlusPreviewActions,
    myAlertOnValium,
    HydratorPlusPlusCanvasFactory
  ) {
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
    this.HydratorPlusPlusPreviewActions = HydratorPlusPlusPreviewActions;
    this.HydratorPlusPlusOrderingFactory = HydratorPlusPlusOrderingFactory;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;
    this.avsc = avsc;
    this.PipelineMetricsStore = window.CaskCommon.PipelineMetricsStore;
    this.HydratorPlusPlusNodeService = HydratorPlusPlusNodeService;
    this.eventEmitter = window.CaskCommon.ee(window.CaskCommon.ee);
    this.configurationGroupUtilities = window.CaskCommon.ConfigurationGroupUtilities;
    this.dynamicFiltersUtilities = window.CaskCommon.DynamicFiltersUtilities;
    this.showNewSchemaEditor = window.localStorage['schema-editor'] === 'true';
    this.myAlertOnValium = myAlertOnValium;
    this.metricsContext = rNodeMetricsContext;
    this.isStudioMode = rIsStudioMode;
    this.rPlugin = rPlugin;
    this.HydratorPlusPlusCanvasFactory = HydratorPlusPlusCanvasFactory;
    this.validatePluginProperties = this.validatePluginProperties.bind(this);
    this.getPreviewId = this.getPreviewId.bind(this);
    this.previewId = this.getPreviewId();
    this.previewStatus = null;
    this.getStagesAndConnections = this.getStagesAndConnections.bind(this);
    this.getIsMacroEnabled = this.getIsMacroEnabled.bind(this);
    this.onImportSchema = this.onImportSchema.bind(this);
    this.onClearSchema = this.onClearSchema.bind(this);
    this.onPropagateSchema = this.onPropagateSchema.bind(this);
    this.onMacroEnabled = this.onMacroEnabled.bind(this);
    this.onSchemaChange = this.onSchemaChange.bind(this);
    this.onSchemaImportLinkClick = this.onSchemaImportLinkClick.bind(this);
    this.isSchemaMacro = this.isSchemaMacro.bind(this);
    this.onPropertiesChange = this.onPropertiesChange.bind(this);
    this.handleLabelChange = this.handleLabelChange.bind(this);
    this.initializeMetrics = this.initializeMetrics.bind(this);
    this.showContents = this.showContents.bind(this);
    this.initializePreview = this.initializePreview.bind(this);
    this.setComments = this.setComments.bind(this);
    this.tabs = [
      {
        label: 'Properties',
        templateUrl: '/assets/features/hydrator/templates/partial/node-config-modal/configuration-tab.html'
      },
      {
        label: 'Preview',
        templateUrl: '/assets/features/hydrator/templates/partial/node-config-modal/preview-tab.html'
      },
      {
        label: 'Documentation',
        templateUrl: '/assets/features/hydrator/templates/partial/node-config-modal/reference-tab.html'
      },
      {
        label: 'Metrics',
        templateUrl: '/assets/features/hydrator/templates/partial/node-config-modal/metrics-tab.html'
      }
    ];
    this.setDefaults();
    this.fetchPluginInfo(rPlugin)
        .then(this.initializeMetrics)
        .then(this.showContents)
        .then(this.initializePreview);

    this.portMetricsToShow = this.PipelineMetricsStore.getState().portsToShow;

    this.$scope.$on('modal.closing', () => {
      this.updateNodeStateIfDirty();
      this.previewStore.dispatch(
        this.HydratorPlusPlusPreviewActions.resetPreviewData()
      );
    });

    // Timeouts
    this.setStateTimeout = null;

    this.eventEmitter.on('dataset.selected', this.handleDatasetSelected.bind(this));

    this.$scope.$on('$destroy', () => {
      this.$timeout.cancel(this.setStateTimeout);
      this.eventEmitter.off('dataset.selected', this.handleDatasetSelected.bind(this));
    });

    this.labelConfig = {
      widgetProperty: {
        label: 'Label',
        'widget-type': 'textbox',
      },
      pluginProperty: {
        required: true,
      }
    };
  }

  fetchPluginInfo(rPlugin) {
    const pluginNode = rPlugin.pluginNode;
    const appType = rPlugin.appType;
    const sourceConnections = rPlugin.sourceConnections;
    const sourceNodes = rPlugin.sourceNodes;
    const artifactVersion = rPlugin.artifactVersion;
    return this.HydratorPlusPlusNodeService
        .getPluginInfo(pluginNode, appType, sourceConnections, sourceNodes, artifactVersion)
        .then((nodeWithInfo) => {
          let pluginType = nodeWithInfo.type || nodeWithInfo.plugin.type;
          return this.setDefaults({
            node: nodeWithInfo,
            isValidPlugin: true,
            type: appType,
            isSource: this.GLOBALS.pluginConvert[pluginType] === 'source',
            isSink: this.GLOBALS.pluginConvert[pluginType] === 'sink',
            isTransform: this.GLOBALS.pluginConvert[pluginType] === 'transform',
            isAction: this.GLOBALS.pluginConvert[pluginType] === 'action',
            isCondition: this.GLOBALS.pluginConvert[pluginType] === 'condition',
          });
        }, (err) => {
          if (err && err.statusCode === 404) {
            // This is when plugin artifact is unavailable. Show appropriate message.
            this.state.configfetched = true;
            this.state.noproperty = 0;
            this.state.isValidPlugin = false;
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
      isAction: config.isAction || false,
      isCondition: config.isCondition || false,

      type: config.appType || null,
      watchers: [],
      outputSchemaUpdate: 0,
      schemaAdvance: false
    };
    this.isPreviewMode = this.previewStore.getState().preview.isPreviewModeEnabled;
    this.isPreviewData = this.previewStore.getState().preview.previewData;
    this.activeTab = 1;
    if (this.isPreviewMode && this.isPreviewData && !this.rPlugin.isAction) {
      this.activeTab = 2;
    } else if (this.PipelineMetricsStore.getState().metricsTabActive) {
      this.activeTab = 4;
    }

    this.defaultState = angular.copy(this.state);

    let propertiesSchema = this.myHelpers.objectQuery(this.state.node, 'plugin', 'properties', 'schema');
    let schemaArr = propertiesSchema || this.state.node.outputSchema;

    if (schemaArr) {
      if (Array.isArray(schemaArr)) {
        angular.forEach(schemaArr, (schemaObj) => {
          if (schemaObj.schema) {
            try {
              this.avsc.parse(schemaObj.schema, { wrapUnions: true });
            } catch (e) {
              // If its old schema editor by default set it to advance
              if (!this.showNewSchemaEditor) {
                this.state.schemaAdvance = true;
              } else {
                // else if its a new schema editor set advance only if the schema is a macro.
                if (schemaArr.indexOf('${') !== -1) {
                  this.state.schemaAdvance = true;
                }
              }
            }
          }
        });
      } else {
        try {
          this.avsc.parse(schemaArr, { wrapUnions: true });
        } catch (e) {
          // If its old schema editor by default set it to advance
          if (!this.showNewSchemaEditor) {
            this.state.schemaAdvance = true;
          } else {
            // else if its a new schema editor set advance only if the schema is a macro.
            if (schemaArr.indexOf('${') !== -1) {
              this.state.schemaAdvance = true;
            }
          }
        }
      }
    }

    this.showPropagateConfirm = false;
  }

  initializeMetrics() {
    this.isMetricsEnabled = this.$scope.isDisabled && (Array.isArray(this.metricsContext.runs) && this.metricsContext.runs.length);
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
  }

  initializePreview() {
    if (this.isStudioMode && this.isPreviewMode && this.previewId) {
      this.previewData = null;
      this.updatePreviewStatus();
      this.selectedNode = {
        nodeType: this.state.node.type,
        name: this.state.node.plugin.label,
        plugin: this.state.node.plugin,
        isSource: this.state.isSource,
        isSink: this.state.isSink,
        isCondition: this.state.isCondition,
       };
    }
  }

  handleDatasetSelected(schema, format, datasetAlreadyExists, datasetId) {
    if (datasetAlreadyExists) {
      this.datasetAlreadyExists = datasetAlreadyExists;
    } else {
      this.datasetAlreadyExists = false;
    }

    // if this plugin is having an existing dataset with a macro, then don't change anything.
    // else if the user is changing to another existing dataset, then show basic mode.
    if (this.myHelpers.objectQuery(this, 'defaultState', 'node', 'plugin', 'properties', 'name') && this.defaultState.node.plugin.properties.name !== datasetId) {
      this.state.schemaAdvance = false;
    }
    if (datasetId) {
      this.datasetId = datasetId;
    }
  }

  onPropertiesChange(values = {}) {
    this.state.node.plugin.properties = values;
  }

  handleLabelChange(value) {
    this.state.node.plugin.label = value;
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
            this.widgetJson = res;

            // Not going to eliminate the groupsConfig just yet, because there are still other things depending on it
            // such as output schema.
            try {
              this.state.groupsConfig = this.HydratorPlusPlusPluginConfigFactory.generateNodeConfig(this.state.node._backendProperties, res);
            } catch (e) {
              noJsonErrorHandler();
              return;
            }

            const generateJumpConfig = (jumpConfig, properties) => {
              let datasets = [];
              let jumpConfigDatasets = jumpConfig.datasets || [];
              datasets = jumpConfigDatasets.map(dataset => ({ datasetId: properties[dataset['ref-property-name']], entityType: 'datasets' }));
              return {datasets};
            };
            if (res.errorDataset || this.state.node.errorDatasetName) {
              this.state.showErrorDataset = true;
              this.state.errorDatasetTooltip = res.errorDataset && res.errorDataset.errorDatasetTooltip || false;
              this.state.node.errorDatasetName = this.state.node.errorDatasetName || '';
            }

            if (this.$scope.isDisabled && this.state.groupsConfig.jumpConfig && Object.keys(this.state.groupsConfig.jumpConfig).length) {
              let {datasets} = generateJumpConfig(this.state.groupsConfig.jumpConfig, this.state.node.plugin.properties);
              this.state.groupsConfig.jumpConfig.datasets = datasets;
            } else {
              // If we isDisabled is set to false then we are in studio mode & hence remove jump config.
              // Jumpconfig is only for published view where everything is disabled.
              delete this.state.groupsConfig.jumpConfig;
            }
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
    this.eventEmitter.emit('schema.clear');
  }

  importFiles(files) {
    let reader = new FileReader();
    reader.readAsText(files[0], 'UTF-8');

    reader.onload = (evt) => {
      let data = evt.target.result;
      this.eventEmitter.emit('schema.import', data);
    };
  }

  onSchemaImportLinkClick() {
    this.$timeout(() => document.getElementById('schema-import-link').click());
  }

  exportSchema() {
    this.eventEmitter.emit('schema.export');
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

  validatePluginProperties(callback, validationFromGetSchema) {
    const nodeInfo = this.HydratorPlusPlusCanvasFactory.pruneProperties({
      stages: [angular.copy(this.state.node)]
    }).stages[0];
    let vm = this;
    vm.propertyErrors = {};
    vm.inputSchemaErrors = {};
    vm.outputSchemaErrors = {};
    if(!validationFromGetSchema){
      vm.validating = true;
      vm.errorCount = undefined;
    }
    const errorCb = ({ errorCount, propertyErrors, inputSchemaErrors, outputSchemaErrors }) => {
      // errorCount can be 0, a positive integer, or undefined (in case of an error thrown)
      vm.validating = false;
      vm.errorCount = errorCount;
      if ( errorCount > 0 ){
        vm.propertyErrors = propertyErrors;
        vm.inputSchemaErrors = inputSchemaErrors;
        vm.outputSchemaErrors = outputSchemaErrors;
      } else if ( errorCount === 0 ){
        // Empty existing errors
        vm.propertyErrors = {};
        vm.inputSchemaErrors = {};
        vm.outputSchemaErrors = {};
        // Do not show success validation message for validation via get schema.
        if (validationFromGetSchema === true) {
          vm.errorCount = undefined;
        }
      } else {
        vm.propertyErrors = propertyErrors;
      }

      if (callback && typeof callback === 'function') {
        callback();
      }
    };
    this.HydratorPlusPlusPluginConfigFactory.validatePluginProperties(nodeInfo, this.state.config, errorCb, validationFromGetSchema);
  }

  // MACRO ENABLED SCHEMA
  toggleAdvance() {
    if (this.state.node.outputSchema.length > 0) {
      try {
        this.avsc.parse(this.state.node.outputSchema[0].schema, { wrapUnions: true });
      } catch (e) {
        this.state.node.outputSchema = [this.HydratorPlusPlusNodeService.getOutputSchemaObj('')];
      }
    }

    this.state.schemaAdvance = !this.state.schemaAdvance;
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

  updatePreviewDataAndStatus(newPreviewData) {
    this.updatePreviewStatus();
    this.previewData = newPreviewData;
  }

  updatePreviewStatus() {
    const previewState = this.previewStore.getState().preview;
    if (previewState.status) {
      this.previewStatus = previewState.status;
    }
  }

  getPreviewId() {
    return this.previewStore.getState().preview.previewId;
  }

  getStagesAndConnections() {
    return this.ConfigStore.getConfigForExport().config;
  }

  // TOOLTIPS FOR DISABLED SCHEMA ACTIONS
  getImportDisabledTooltip() {
    if (this.datasetAlreadyExists) {
      return `The dataset '${this.datasetId}' already exists. Its schema cannot be modified.`;
    } else if (this.state.schemaAdvance) {
      return 'Importing a schema in Advanced mode is not supported';
    }
    return '';
  }

  getPropagateDisabledTooltip() {
    if (this.state.node.type === 'splittertransform') {
      return 'Propagating a schema with Splitter plugins is currently not supported';
    } else if (this.state.schemaAdvance) {
      return 'Propagating a schema in Advanced mode is not supported';
    }
    return '';
  }

  getClearDisabledTooltip() {
    if (this.datasetAlreadyExists) {
      return `The dataset '${this.datasetId}' already exists. Its schema cannot be cleared.`;
    } else if (this.state.schemaAdvance) {
      return 'Clearing a schema in Advanced mode is not supported';
    }
    return '';
  }

  getIsMacroEnabled() {
    return (
      !this.$scope.isDisabled &&
      this.state.node._backendProperties['schema'] &&
      this.state.node._backendProperties['schema'].macroSupported
    );
  }

  onClearSchema() {
    this.state.node['outputSchema'] = [{ name: 'etlSchemaBody', schema: ''}];
    this.updateAngularPostSchemaUpdate();
  }

  onPropagateSchema() {
    this.showPropagateConfirm = true;
    this.updateAngularPostSchemaUpdate();
  }

  onMacroEnabled() {
    this.state.schemaAdvance = !this.state.schemaAdvance;
    this.updateAngularPostSchemaUpdate();
  }

  onSchemaChange(outputSchemas) {
    this.state.node.outputSchema = outputSchemas;
    this.updateAngularPostSchemaUpdate();
  }

  onImportSchema(stringifiedSchema) {
    try {
      this.state.node.outputSchema = JSON.parse(stringifiedSchema);
      if (!Array.isArray(this.state.node.outputSchema)) {
        this.state.node.outputSchema = [this.state.node.outputSchema];
        this.updateAngularPostSchemaUpdate();
      }
    } catch(e) {
      this.state.node.outputSchema = [{ name: 'etlSchemaBody', schema: ''}];
      this.updateAngularPostSchemaUpdate();
    }
  }

  updateAngularPostSchemaUpdate() {
    try {
      this.$scope.$digest();
    } catch(e) {
      return;
    }
  }

  isSchemaMacro() {
    return this.state.schemaAdvance;
  }

  getActionsDropdownMap(isInputSchema) {
    let actionsMap = {};
    if (isInputSchema) {
      return {};
    }
    if (this.$scope.isDisabled) {
      return {
        export: {
          value: 'export',
          label: 'Export',
          disabled: this.state.schemaAdvance,
          tooltip: this.state.schemaAdvance ? 'Exporting a schema in Advanced mode is not supported' : '',
          onClick: this.exportSchema.bind(this),
        }
      };
    }
    if (this.state.groupsConfig.outputSchema.implicitSchema) {
      return {
        export: {
          value: 'export',
          label: 'Export',
          disabled: this.state.schemaAdvance,
          tooltip: this.state.schemaAdvance ? 'Exporting a schema in Advanced mode is not supported' : '',
          onClick: this.exportSchema.bind(this),
        },
        propagate: {
          value: 'propagate',
          label: 'Propagate',
          disabled:
            this.state.schemaAdvance ||
            this.state.node.type === 'splittertransform',
          tooltip: this.getPropagateDisabledTooltip(),
          onClick: this.onPropagateSchema.bind(this),
        },
      };
    }
    if (this.getIsMacroEnabled()) {
      actionsMap['macro'] = {
        value: 'macro',
        label: this.state.schemaAdvance ? 'Editor' : 'Macro',
        disabled: this.datasetAlreadyExists,
        tooltip: this.datasetAlreadyExists ? `The dataset '${this.datasetId}' already exists. Its schema cannot be modified.` : '',
        onClick: this.onMacroEnabled.bind(this),
      };
    }
    actionsMap = Object.assign({}, actionsMap, {
      import: {
        value: 'import',
        label: 'Import',
        disabled: this.datasetAlreadyExists || this.state.schemaAdvance,
        tooltip: this.getImportDisabledTooltip(),
        onClick: this.onSchemaImportLinkClick.bind(this),
      },
      export: {
        value: 'export',
        label: 'Export',
        disabled: this.state.schemaAdvance,
        tooltip: this.state.schemaAdvance ? 'Exporting a schema in Advanced mode is not supported' : '',
        onClick: this.exportSchema.bind(this),
      },
      propagate: {
        value: 'propagate',
        label: 'Propagate',
        disabled:
          this.state.schemaAdvance ||
          this.state.node.type === 'splittertransform',
        tooltip: this.getPropagateDisabledTooltip(),
        onClick: this.onPropagateSchema.bind(this),
      },
      clear: {
        value: 'clear',
        label: 'Clear',
        disabled: this.datasetAlreadyExists || this.state.schemaAdvance,
        tooltip: this.getClearDisabledTooltip(),
        onClick: this.onClearSchema.bind(this),
      },
    });
    return actionsMap;
  }

  setComments(nodeId, comments) {
    this.state.node.information = this.state.node.information || {};
    this.state.node.information.comments = { list: comments };
  }
}

angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorPlusPlusNodeConfigCtrl', HydratorPlusPlusNodeConfigCtrl);
