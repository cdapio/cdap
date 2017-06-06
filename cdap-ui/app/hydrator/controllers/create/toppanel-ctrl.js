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

class HydratorPlusPlusTopPanelCtrl {
  constructor($stateParams, HydratorPlusPlusConfigStore, HydratorPlusPlusConfigActions, $uibModal, HydratorPlusPlusConsoleActions, DAGPlusPlusNodesActionsFactory, GLOBALS, myHelpers, HydratorPlusPlusConsoleStore, myPipelineExportModalService, $timeout, $scope, HydratorPlusPlusPreviewStore, HydratorPlusPlusPreviewActions, $interval, myPipelineApi, $state, MyCDAPDataSource, myAlertOnValium, MY_CONFIG, PREVIEWSTORE_ACTIONS, $q, NonStorePipelineErrorFactory, rArtifacts,  $window, LogViewerStore, LOGVIEWERSTORE_ACTIONS, myPreviewLogsApi, DAGPlusPlusNodesStore, myPreferenceApi, HydratorPlusPlusHydratorService, $rootScope, uuid) {
    this.consoleStore = HydratorPlusPlusConsoleStore;
    this.myPipelineExportModalService = myPipelineExportModalService;
    this.HydratorPlusPlusConfigStore = HydratorPlusPlusConfigStore;
    this.GLOBALS = GLOBALS;
    this.HydratorPlusPlusConfigActions = HydratorPlusPlusConfigActions;
    this.$uibModal = $uibModal;
    this.HydratorPlusPlusConsoleActions = HydratorPlusPlusConsoleActions;
    this.DAGPlusPlusNodesActionsFactory = DAGPlusPlusNodesActionsFactory;
    this.parsedDescription = this.HydratorPlusPlusConfigStore.getDescription();
    this.myHelpers = myHelpers;
    this.$timeout = $timeout;
    this.PREVIEWSTORE_ACTIONS = PREVIEWSTORE_ACTIONS;
    this.previewStore = HydratorPlusPlusPreviewStore;
    this.previewActions = HydratorPlusPlusPreviewActions;
    this.LOGVIEWERSTORE_ACTIONS = LOGVIEWERSTORE_ACTIONS;
    this.LogViewerStore = LogViewerStore;
    this.$interval = $interval;
    this.myPipelineApi = myPipelineApi;
    this.myPreviewLogsApi = myPreviewLogsApi;
    this.myPreferenceApi = myPreferenceApi;
    this.DAGPlusPlusNodesStore = DAGPlusPlusNodesStore;
    this.$state = $state;
    this.dataSrc = new MyCDAPDataSource($scope);
    this.myAlertOnValium = myAlertOnValium;
    this.currentPreviewId = null;
    this.$window = $window;
    this.viewConfig = false;
    this.viewScheduler = false;
    this.viewLogs = false;
    this.$q = $q;
    this.NonStorePipelineErrorFactory = NonStorePipelineErrorFactory;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.artifacts = rArtifacts;
    this.$rootScope = $rootScope;
    this.uuid = uuid;
    this.macrosMap = {};
    this.resolvedMacros = {};
    this.userRuntimeArgumentsMap = {};
    this.runtimeArguments = {};
    this.validToStartPreview = true;
    this.$stateParams = $stateParams;
    this.setState();
    this.setActiveNodes();
    this.HydratorPlusPlusConfigStore.registerOnChangeListener(this.setState.bind(this));
    this.DAGPlusPlusNodesStore.registerOnChangeListener(this.setActiveNodes.bind(this));
    this.focusTimeout = null;
    this.timeoutInMinutes = 2;

    if ($stateParams.isClone) {
      this.openMetadata();
    }

    this.currentDraftId = this.HydratorPlusPlusConfigStore.getDraftId();
    if (this.currentDraftId && this.currentDraftId === this.$window.localStorage.getItem('LastDraftId')) {
      this.currentPreviewId = this.$window.localStorage.getItem('LastPreviewId');
      this.previewStore.dispatch(
        this.previewActions.setPreviewId(this.currentPreviewId)
      );
    }

    this.isPreviewEnabled = angular.isObject(MY_CONFIG.hydrator) &&
                            MY_CONFIG.hydrator.previewEnabled === true;

    this.previewMode = false;
    this.previewLoading = true;

    if (this.currentPreviewId) {
      this.myPreviewLogsApi.getLogsStatus({
        namespace : this.$state.params.namespace,
        previewId : this.currentPreviewId
      }).$promise.then(
        (statusRes) => {
          this.previewStartTime = statusRes.startTime;
          this.previewLoading = false;

          // TODO: Just realized it makes more sense to have preview status be in previewStore (HydratorPlusPlusPreviewStore),
          // instead of LogViewerStore. Will migrate later, as I don't want to introduce breaking changes right now.
          this.LogViewerStore.dispatch({
            type: this.LOGVIEWERSTORE_ACTIONS.SET_STATUS,
            payload: {
              status: statusRes.status,
              startTime: statusRes.startTime,
              endTime: statusRes.endTime
            }
          });

          if (statusRes.status === 'RUNNING') {
            this.previewRunning = true;
            this.startTimer();
            this.startPollPreviewStatus(this.currentPreviewId);
          } else {
            this.calculateDuration(statusRes.endTime);
          }
        },
        (statusErr) => {
          console.log('ERROR: ', statusErr);
          this.setDefault();
        }
      );
    } else {
      this.setDefault();
    }

    let unsub = this.previewStore.subscribe(() => {
      let state = this.previewStore.getState().preview;
      this.previewMode = state.isPreviewModeEnabled;
      this.macrosMap = state.macros;
      this.userRuntimeArgumentsMap = state.userRuntimeArguments;
      this.timeoutInMinutes = state.timeoutInMinutes;
    });

    this.macrosMap = this.previewStore.getState().preview.macros;
    this.userRuntimeArgumentsMap = this.previewStore.getState().preview.userRuntimeArguments;

    if (Object.keys(this.macrosMap).length === 0) {
      this.fetchMacros();
    }

    $scope.$on('$destroy', () => {
      unsub();
      this.stopPreview();
      this.previewStore.dispatch(
        this.previewActions.togglePreviewMode(false)
      );
      this.$interval.cancel(this.previewTimerInterval);
      this.$timeout.cancel(this.focusTimeout);
    });
  }

  setDefault() {
    this.previewStartTime = null;
    this.displayDuration = {
      minutes: '--',
      seconds: '--'
    };
    this.previewTimerInterval = null;
    this.previewLoading = false;
    this.previewRunning = false;
  }

  setMetadata(metadata) {
    this.state.metadata = metadata;
  }
  setState() {
    this.state = {
      metadata: {
        name: this.HydratorPlusPlusConfigStore.getName(),
        description: this.HydratorPlusPlusConfigStore.getDescription()
      },
      viewSettings: this.myHelpers.objectQuery(this.state, 'viewSettings') || false,
      artifact: this.HydratorPlusPlusConfigStore.getArtifact()
    };
  }
  setActiveNodes() {
    this.hasNodes = !!this.DAGPlusPlusNodesStore.getNodes().length;
    this.fetchMacros();
  }

  openMetadata() {
    this.metadataExpanded = true;
    this.invalidName = false;

    this.$timeout.cancel(this.focusTimeout);
    this.focusTimeout = this.$timeout(() => {
      document.getElementById('pipeline-name-input').focus();
    });
  }
  resetMetadata(event) {
    this.setState();
    this.metadataExpanded = false;
    event.preventDefault();
    event.stopPropagation();
  }
  saveMetadata(event) {
    this.HydratorPlusPlusConfigActions.setMetadataInfo(this.state.metadata.name, this.state.metadata.description);
    if (this.state.metadata.description) {
      this.parsedDescription = this.state.metadata.description.replace(/\n/g, ' ');
      this.tooltipDescription = this.state.metadata.description.replace(/\n/g, '<br />');
    } else {
      this.parsedDescription = '';
      this.tooltipDescription = '';
    }
    this.metadataExpanded = false;
    event.preventDefault();
    event.stopPropagation();
  }
  onEnterOnMetadata(event) {
    // Save when user hits ENTER key.
    if (event.keyCode === 13) {
      this.saveMetadata(event);
      this.metadataExpanded = false;
    } else if (event.keyCode === 27) {
      // Reset if the user hits ESC key.
      this.resetMetadata(event);
    }
  }

  onImport() {
    let fileBrowserClickCB = () => {
      document.getElementById('pipeline-import-config-link').click();
    };
    // This is not using the promise pattern as browsers NEED to have the click on the call stack to generate the click on input[type=file] button programmatically in like line:115.
    // When done in promise we go into the promise ticks and the then callback is called in the next tick which prevents the browser to open the file dialog
    // as a file dialog is opened ONLY when manually clicked by the user OR transferring the click to another button in the same call stack
    // TL;DR Can't open file dialog programmatically. If we need to, we need to transfer the click from a user on a button directly into the input file dialog button.
    this._checkAndShowConfirmationModalOnDirtyState(fileBrowserClickCB);
  }

  onExport() {
    this.DAGPlusPlusNodesActionsFactory.resetSelectedNode();
    let config = angular.copy(this.HydratorPlusPlusConfigStore.getDisplayConfig());
    let exportConfig = this.HydratorPlusPlusConfigStore.getConfigForExport();
    delete exportConfig.__ui__;
    this.myPipelineExportModalService.show(config, exportConfig);
  }
  onSaveDraft() {
    this.HydratorPlusPlusConfigActions.saveAsDraft();
    this.checkNameError();
    this.$window.localStorage.setItem('LastDraftId', this.HydratorPlusPlusConfigStore.getDraftId());
    this.$window.localStorage.setItem('LastPreviewId', this.currentPreviewId);
  }
  checkNameError() {
    let messages = this.consoleStore.getMessages() || [];
    let filteredMessages = messages.filter( message => {
      return ['MISSING-NAME', 'INVALID-NAME'].indexOf(message.type) !== -1;
    });

    this.invalidName = (filteredMessages.length ? true : false);
  }
  onPublish() {
    this.HydratorPlusPlusConfigActions.publishPipeline();
    this.checkNameError();
  }
  showSettings() {
    this.state.viewSettings = !this.state.viewSettings;
  }

  // PREVIEW
  startTimer() {
    this.previewTimerInterval = this.$interval(() => {
      this.calculateDuration();
    }, 500);
  }
  stopTimer() {
    this.$interval.cancel(this.previewTimerInterval);
  }
  calculateDuration(endTime) {
    if (!endTime) {
      endTime = new Date();
    }
    let duration = (endTime - this.previewStartTime) / 1000;
    duration = duration >= 0 ? duration : 0;

    let minutes = Math.floor(duration / 60);
    let seconds = Math.floor(duration % 60);
    seconds = seconds < 10 ? '0' + seconds : seconds;
    minutes = minutes < 10 ? '0' + minutes : minutes;

    this.displayDuration = {
      minutes: minutes,
      seconds: seconds
    };
  }

  fetchMacros() {
    let newMacrosMap = {};
    let nodes = this.HydratorPlusPlusConfigStore.getStages();
    let listOfPromises = [];
    for (let i = 0; i < nodes.length; i++) {
      let node = nodes[i];
      let backendProperties = this.myHelpers.objectQuery(node, '_backendProperties');
      if (angular.isObject(backendProperties) && Object.keys(backendProperties).length) {
        listOfPromises.push(this.$q.when(node));
      } else {
        listOfPromises.push(this.HydratorPlusPlusHydratorService.fetchBackendProperties(node, this.state.artifact.name));
      }
    }

    this.$q.all(listOfPromises)
    .then(() => {
      for (let i = 0; i < nodes.length; i++) {
        let properties = this.myHelpers.objectQuery(nodes[i], 'plugin', 'properties');
        let backendProperties = this.myHelpers.objectQuery(nodes[i], '_backendProperties');
        for (let prop in properties) {
          if (properties.hasOwnProperty(prop) &&
                backendProperties &&
                backendProperties.hasOwnProperty(prop) &&
                backendProperties[prop].macroSupported) {
            let macroString = properties[prop];
            /* Can handle:
              - Simple nested macro (e.g. '${function(${macro1})}')
              - Multiple macros (e.g. '${macro1}${macro2}')
              - And combined (e,g, '${function(${macro1})}${macro2}')
              More complicated cases will be handled by the backend
            */
            if (macroString.indexOf('${') !== -1 &&
              macroString.indexOf('}') !== -1
            ) {
              let macroKeys = [];
              let currentMacroDepth = 0;
              let maxMacroDepth = 0;
              let lastClosingBraceIndex = 0;
              for (let i = macroString.length - 1; i >=1; i--) {
                let macroChar = macroString[i];
                if (macroChar === '}') {
                  lastClosingBraceIndex = i;
                  currentMacroDepth += 1;
                }
                if (macroChar === '{' && macroString[i-1] === '$') {
                  currentMacroDepth -= 1;
                  if (currentMacroDepth >= maxMacroDepth) {
                    maxMacroDepth = currentMacroDepth;
                    let macroKey = macroString.substring(i + 1, lastClosingBraceIndex);
                    macroKeys.push(macroKey);
                  }
                }
              }
              macroKeys.forEach((key) => {
                newMacrosMap[key] = '';
              });
            }
          }
        }
      }

      if (Object.keys(newMacrosMap).length > 0) {
        /*
          Will resolve macros from preferences, if the new macro object is different than
          the one we already have (this.macrosMap). We have a new macro object when the
          user adds or removes macro(s) from the config of a stage.
        */

        let differentMacroKeys = false;
        if (Object.keys(newMacrosMap).length !== Object.keys(this.macrosMap).length) {
          differentMacroKeys = true;
        } else {
          for (let macroKey in newMacrosMap) {
            if (newMacrosMap.hasOwnProperty(macroKey) && !this.macrosMap.hasOwnProperty(macroKey)) {
              differentMacroKeys = true;
              break;
            }
          }
        }

        if (differentMacroKeys) {
          this.getRuntimeArguments(newMacrosMap);
        }
      } else {
        this.getRuntimeArguments(newMacrosMap);
      }
    });
  }

  getRuntimeArguments(newMacrosMap = this.macrosMap) {

    // if there are no runtime arguments at all
    if (Object.keys(newMacrosMap).length === 0 && Object.keys(this.userRuntimeArgumentsMap).length === 0) {
      this.macrosMap = newMacrosMap;
      this.previewStore.dispatch(
        this.previewActions.setMacros(this.macrosMap)
      );
      this.runtimeArguments.pairs = [{
        key: '',
        value: '',
        uniqueId: 'id-' + this.uuid.v4()
      }];
      this.validToStartPreview = this.isValidToStartPreview();
      return this.$q.when(this.runtimeArguments);
    }

    this.macrosMap = this.previewStore.getState().preview.macros;
    this.userRuntimeArgumentsMap = this.previewStore.getState().preview.userRuntimeArguments;

    // if there are non-zero number of macros
    if (Object.keys(newMacrosMap).length !== 0) {
      let preferenceParam = {
        namespace: this.$state.params.namespace
      };

      return this.myPreferenceApi
        .getNamespacePreferenceResolved(preferenceParam)
        .$promise
        .then(
          (res) => {
            let newResolvedMacros = this.HydratorPlusPlusHydratorService.getPrefsRelevantToMacros(res, this.macrosMap);
            let newPrefs = {};

            // if the higher level preferences have changed
            if (!angular.equals(newResolvedMacros, this.resolvedMacros)) {
              for (let macroKey in newResolvedMacros) {
                if (newResolvedMacros.hasOwnProperty(macroKey) &&
                  this.resolvedMacros.hasOwnProperty(macroKey) &&
                  this.macrosMap.hasOwnProperty(macroKey)) {
                  if (newResolvedMacros[macroKey] !== this.resolvedMacros[macroKey] &&
                      this.resolvedMacros[macroKey] === this.macrosMap[macroKey]) {
                    newPrefs[macroKey] = newResolvedMacros[macroKey];
                  }
                }
              }
              this.resolvedMacros = newResolvedMacros;
            }

            if (!angular.equals(newMacrosMap, this.macrosMap) || Object.keys(newPrefs).length > 0) {
              // if user added or removed macros in the stage config
              if (!angular.equals(newMacrosMap, this.macrosMap)) {
                this.resolvedMacros = Object.assign({}, this.HydratorPlusPlusHydratorService.getPrefsRelevantToMacros(res, newMacrosMap));
                this.macrosMap = Object.assign({}, newMacrosMap, this.resolvedMacros);
              }
              // only update the macros that have new resolved values
              if (Object.keys(newPrefs).length > 0) {
                this.resolvedMacros = Object.assign({}, this.resolvedMacros, newResolvedMacros);
                this.macrosMap = Object.assign({}, this.macrosMap, newPrefs);
              }
              this.previewStore.dispatch(
                this.previewActions.setMacros(this.macrosMap)
              );
            }
            this.runtimeArguments = this.HydratorPlusPlusHydratorService.convertMacrosToRuntimeArguments(this.runtimeArguments, this.macrosMap, this.userRuntimeArgumentsMap);
            this.validToStartPreview = this.isValidToStartPreview();
            return this.runtimeArguments;
          },
          err => {
            console.log('ERROR', err);
          }
        );

    // if there are zero macros, but there are user-set runtime arguments
    } else {
      this.macrosMap = newMacrosMap;
      this.previewStore.dispatch(
        this.previewActions.setMacros(this.macrosMap)
      );
      this.runtimeArguments = this.HydratorPlusPlusHydratorService.convertMacrosToRuntimeArguments(this.runtimeArguments, this.macrosMap, this.userRuntimeArgumentsMap);
      this.validToStartPreview = this.isValidToStartPreview();
      return this.$q.when(this.runtimeArguments);
    }
  }

  toggleConfig() {
    this.getRuntimeArguments()
    .then(() => {
      this.viewConfig = !this.viewConfig;
    });
  }

  startOrStopPreview() {
    if (this.validToStartPreview) {
      this.getRuntimeArguments()
      .then(() => {
        if (this.previewRunning) {
          this.stopPreview();
        } else {
          this.onPreviewStart();
        }
      });
    } else {
      this.toggleConfig();
    }
  }

  toggleScheduler() {
    this.viewScheduler = !this.viewScheduler;
  }

  applyRuntimeArguments() {
    let macros = this.HydratorPlusPlusHydratorService.convertRuntimeArgsToMacros(this.runtimeArguments);
    this.macrosMap = macros.macrosMap;
    this.userRuntimeArgumentsMap = macros.userRuntimeArgumentsMap;
    // have to do this because cannot do two `this.previewStore.dispatch` in a row
    this.previewStore.dispatch(
      this.previewActions.setMacrosAndUserRuntimeArgs(this.macrosMap, this.userRuntimeArgumentsMap)
    );
    this.validToStartPreview = this.isValidToStartPreview();
  }

  isValidToStartPreview() {
    return !this.HydratorPlusPlusHydratorService.keyValuePairsHaveMissingValues(this.runtimeArguments);
  }

  onPreviewStart() {
    this._checkAndShowConfirmationModalOnActionPlugin(this.runPreview());
  }

  runPreview() {
    this.previewLoading = true;
    this.loadingLabel = 'Starting';
    this.viewConfig = false;

    this.displayDuration = {
      minutes: '--',
      seconds: '--'
    };

    this.currentPreviewId = null;
    this.previewStore.dispatch(
      this.previewActions.setPreviewId(this.currentPreviewId)
    );
    this.$window.localStorage.removeItem('LastPreviewId', this.currentPreviewId);

    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope
    };

    // GENERATING PREVIEW CONFIG
    // This might/should be extracted out to a factory

    let pipelineConfig = this.HydratorPlusPlusConfigStore.getConfigForExport();
    /**
     *  This is a cheat way for generating preview for the entire pipeline
     **/

    let macrosWithNonEmptyValues = this.HydratorPlusPlusHydratorService.getMacrosWithNonEmptyValues(this.macrosMap);
    let previewConfig = {
      startStages: [],
      endStages: [],
      runtimeArgs: Object.assign({}, macrosWithNonEmptyValues, this.userRuntimeArguments)
    };

    if (this.state.artifact.name === this.GLOBALS.etlDataPipeline) {
      pipelineConfig.preview = Object.assign({}, previewConfig, {
        'realDatasets': [],
        'programName': 'DataPipelineWorkflow',
        'programType': 'Workflow'
      });
    } else if (this.state.artifact.name === this.GLOBALS.etlDataStreams) {
      pipelineConfig.preview = Object.assign({}, previewConfig, {
        'realDatasets':[],
        'programName': 'DataStreamsSparkStreaming',
        'programType': 'Spark',
        'timeout': this.timeoutInMinutes
      });
    }
    // Get start stages and end stages
    // Current implementation:
    //    - start stages mean sources
    //    - end stages mean sinks
    angular.forEach(pipelineConfig.config.stages, (node) => {
      if (this.GLOBALS.pluginConvert[node.plugin.type] === 'source') {
        previewConfig.startStages.push(node.name);
      } else if (this.GLOBALS.pluginConvert[node.plugin.type] === 'sink') {
        previewConfig.endStages.push(node.name);
      }
    });
    pipelineConfig.config.preview = previewConfig;

    if (previewConfig.startStages.length === 0 || previewConfig.endStages.length === 0) {
      this.myAlertOnValium.show({
        type: 'danger',
        content: this.GLOBALS.en.hydrator.studio.error.PREVIEW['NO-SOURCE-SINK']
      });
      this.previewLoading = false;
      return;
    }

    this.myPipelineApi.runPreview(params, pipelineConfig).$promise
      .then((res) => {
        this.previewStore.dispatch(
          this.previewActions.setPreviewId(res.application)
        );
        let startTime = new Date();
        this.previewStartTime = startTime;
        this.startTimer();
        this.previewStore.dispatch(
          this.previewActions.setPreviewStartTime(startTime)
        );
        this.currentPreviewId = res.application;
        this.$window.localStorage.setItem('LastDraftId', this.HydratorPlusPlusConfigStore.getDraftId());
        this.$window.localStorage.setItem('LastPreviewId', this.currentPreviewId);
        this.startPollPreviewStatus(res.application);
      }, (err) => {
        this.previewLoading = false;
        this.myAlertOnValium.show({
          type: 'danger',
          content: err.data
        });
      });
  }

  stopPreview() {
    if (!this.currentPreviewId) {
      return;
    }
    let params = {
      namespace: this.$state.params.namespace,
      scope: this.$scope,
      previewId: this.currentPreviewId
    };
    this.previewLoading = true;
    this.loadingLabel = 'Stopping';
    this.stopTimer();
    this.myPipelineApi
        .stopPreview(params, {})
        .$promise
        .then(() => {
          this.previewLoading = false;
          this.previewRunning = false;
        });
  }

  startPollPreviewStatus(previewId) {
    this.previewLoading = false;
    this.previewRunning = true;
    let poll = this.dataSrc.poll({
      _cdapNsPath: '/previews/' + previewId + '/status',
      interval: 1000
    }, (res) => {
      if (this.LogViewerStore) {
        this.LogViewerStore.dispatch({
          type: this.LOGVIEWERSTORE_ACTIONS.SET_STATUS,
          payload: {
            status: res.status,
            startTime: res.startTime,
            endTime: res.endTime
          }
        });
      }
      if (res.status !== 'RUNNING' && res.status !== 'STARTED') {
        this.stopTimer();
        this.previewRunning = false;
        this.dataSrc.stopPoll(res.__pollId__);
        let pipelinePreviewPlaceholder = 'The preview of the pipeline';
        let pipelineName = this.HydratorPlusPlusConfigStore.getName();
        if (pipelineName.length > 0) {
          pipelinePreviewPlaceholder += ` "${pipelineName}"`;
        }
        if (res.status === 'COMPLETED' || res.status === 'KILLED_BY_TIMER') {
          this.myAlertOnValium.show({
            type: 'success',
            content: `${pipelinePreviewPlaceholder} has completed successfully.`
          });
        } else if (res.status === 'STOPPED' || res.status === 'KILLED') {
          this.myAlertOnValium.show({
            type: 'success',
            content: `${pipelinePreviewPlaceholder} was stopped.`
          });
        } else if (res.status === 'FAILED' || res.status === 'RUN_FAILED') {
          this.myAlertOnValium.show({
            type: 'danger',
            content: `${pipelinePreviewPlaceholder} has failed. Please check the logs for more information.`
          });
        }
      }
    }, (err) => {
      this.stopTimer();
        this.myAlertOnValium.show({
          type: 'danger',
          content: 'Pipeline preview failed : ' + err
        });
      this.previewRunning = false;
      this.dataSrc.stopPoll(poll.__pollId__);
    });
  }

  togglePreviewMode() {
    this.previewStore.dispatch(
      this.previewActions.togglePreviewMode(!this.previewMode)
    );
  }

  importFile(files) {
    if (files[0].name.indexOf('.json') === -1) {
      this.myAlertOnValium.show({
        type: 'danger',
        content: 'Pipeline configuration should be JSON.'
      });
      return;
    }
    let generateLinearConnections = (config) => {
      let nodes = config.stages;
      let connections = [];
      let i;

      for (i=0; i<nodes.length - 1; i++) {
        connections.push({ from: nodes[i].name, to: nodes[i+1].name });
      }
      return connections;
    };

    let isValidArtifact = (importArtifact) => {
      let isVersionExists = [];
      let isScopeExists = [];
      let isNameExists = this.artifacts.filter( artifact => artifact.name === importArtifact.name );
      isVersionExists = isNameExists.filter( artifact => artifact.version === importArtifact.version );
      isScopeExists = isNameExists.filter( artifact => artifact.scope.toUpperCase() === importArtifact.scope.toUpperCase() );
      return {
        name: isNameExists.length > 0,
        version: isVersionExists.length > 0,
        scope: isScopeExists.length > 0
      };
    };

    var reader = new FileReader();
    reader.readAsText(files[0], 'UTF-8');

    reader.onload =  (evt) => {
      var data = evt.target.result;
      var jsonData;
      try {
        jsonData = JSON.parse(data);
      } catch (e) {
        this.myAlertOnValium.show({
          type: 'danger',
          content: 'Syntax Error. Ill-formed pipeline configuration.'
        });
        return;
      }

      let isNotValid = this.NonStorePipelineErrorFactory.validateImportJSON(jsonData);

      if (isNotValid) {
        this.myAlertOnValium.show({
          type: 'danger',
          content: isNotValid
        });
        return;
      }

      if (!jsonData.config.connections) {
        jsonData.config.connections = generateLinearConnections(jsonData.config);
      }
      let invalidFields = [];
      let isVersionInRange = this.HydratorPlusPlusHydratorService
        .isVersionInRange({
          supportedVersion: this.$rootScope.cdapVersion,
          versionRange: jsonData.artifact.version
        });
      if (isVersionInRange) {
        jsonData.artifact.version = this.$rootScope.cdapVersion;
      } else {
        invalidFields.push(
          `The available version of CDAP Data Pipeline (${this.$rootScope.cdapVersion}) does not support the range of versions specified in the imported pipeline - ${jsonData.artifact.version}. Please update the version ranges in the JSON, and try importing again.`
        );
        this.myAlertOnValium.show({
          type: 'danger',
          content: `${invalidFields}.`
        });
        return;
      }
      let validArtifact = isValidArtifact(jsonData.artifact);
      if (!validArtifact.name || !validArtifact.version || !validArtifact.scope) {
        if (!validArtifact.name) {
          invalidFields.push('Artifact name: ' + jsonData.artifact.name);
        } else {
          if (!validArtifact.version) {
            invalidFields.push('Artifact version: ' + jsonData.artifact.version);
          }
          if (!validArtifact.scope) {
            invalidFields.push('Artifact scope: ' + jsonData.artifact.scope);
          }
        }
        invalidFields = invalidFields.length === 1 ? invalidFields[0] : invalidFields.join(', ');
        this.myAlertOnValium.show({
          type: 'danger',
          content: `Imported pipeline has invalid artifact information: ${invalidFields}.`
        });
      } else {
        if (!jsonData.config.connections) {
          jsonData.config.connections = generateLinearConnections(jsonData.config);
        }
        this.HydratorPlusPlusConfigStore.setState(this.HydratorPlusPlusConfigStore.getDefaults());
        this.$state.go('hydrator.create', { data: jsonData });
      }
    };
  }

  _checkAndShowConfirmationModalOnDirtyState(proceedCb) {
    let goTonextStep = true;
    let isStoreDirty = this.HydratorPlusPlusConfigStore.getIsStateDirty();
    if (isStoreDirty) {
      return this.$uibModal.open({
          templateUrl: '/assets/features/hydrator/templates/create/popovers/canvas-overwrite-confirmation.html',
          size: 'lg',
          backdrop: 'static',
          keyboard: false,
          windowTopClass: 'confirm-modal hydrator-modal center',
          controller: ['$scope', 'HydratorPlusPlusConfigStore', 'HydratorPlusPlusConfigActions', function($scope, HydratorPlusPlusConfigStore, HydratorPlusPlusConfigActions) {
            $scope.isSaving = false;
            $scope.discard = () => {
              goTonextStep = true;
              if (proceedCb) {
                proceedCb();
              }
              $scope.$close();
            };
            $scope.save = () => {
              let pipelineName = HydratorPlusPlusConfigStore.getName();
              if (!pipelineName.length) {
                HydratorPlusPlusConfigActions.saveAsDraft();
                goTonextStep = false;
                $scope.$close();
                return;
              }
              var unsub = HydratorPlusPlusConfigStore.registerOnChangeListener( () => {
                let isStateDirty = HydratorPlusPlusConfigStore.getIsStateDirty();
                // This is solely used for showing the spinner icon until the modal is closed.
                if (!isStateDirty) {
                  unsub();
                  goTonextStep = true;
                  $scope.$close();
                }
              });
              HydratorPlusPlusConfigActions.saveAsDraft();
              $scope.isSaving = true;
            };
            $scope.cancel = () => {
              $scope.$close();
              goTonextStep = false;
            };
          }]
        })
        .closed
        .then(() => {
          return goTonextStep;
        });
    } else {
      if (proceedCb) {
        proceedCb();
      }
      return this.$q.when(goTonextStep);
    }
  }

  _checkAndShowConfirmationModalOnActionPlugin(proceedCb) {
    let config = this.HydratorPlusPlusConfigStore.getConfigForExport().config;

    let actions = config.stages.filter((stage) => {
      return stage.plugin.type === 'action';
    });

    let postActions = config.postActions;

    if (actions.length > 0 || postActions.length > 0) {
      this.viewConfig = false;
      let confirmModal = this.$uibModal.open({
          templateUrl: '/assets/features/hydrator/templates/create/popovers/run-preview-action-confirmation-modal.html',
          size: 'lg',
          backdrop: 'static',
          keyboard: false,
          windowTopClass: 'confirm-modal hydrator-modal center'
        });

      confirmModal.result.then((confirm) => {
        if (confirm && proceedCb) {
          proceedCb();
        }
      });

    } else {
      if (proceedCb) {
        proceedCb();
      }
    }
  }
}

HydratorPlusPlusTopPanelCtrl.$inject = ['$stateParams', 'HydratorPlusPlusConfigStore', 'HydratorPlusPlusConfigActions', '$uibModal', 'HydratorPlusPlusConsoleActions', 'DAGPlusPlusNodesActionsFactory', 'GLOBALS', 'myHelpers', 'HydratorPlusPlusConsoleStore', 'myPipelineExportModalService', '$timeout', '$scope', 'HydratorPlusPlusPreviewStore', 'HydratorPlusPlusPreviewActions', '$interval', 'myPipelineApi', '$state', 'MyCDAPDataSource', 'myAlertOnValium', 'MY_CONFIG', 'PREVIEWSTORE_ACTIONS', '$q', 'NonStorePipelineErrorFactory', 'rArtifacts', '$window', 'LogViewerStore', 'LOGVIEWERSTORE_ACTIONS', 'myPreviewLogsApi', 'DAGPlusPlusNodesStore', 'myPreferenceApi', 'HydratorPlusPlusHydratorService', '$rootScope', 'uuid'];
angular.module(PKG.name + '.feature.hydrator')
  .controller('HydratorPlusPlusTopPanelCtrl', HydratorPlusPlusTopPanelCtrl);
