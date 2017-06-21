/*
* Copyright © 2017 Cask Data, Inc.
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

class MyRealtimePipelineConfigCtrl {
  constructor(uuid, HydratorPlusPlusHydratorService, HYDRATOR_DEFAULT_VALUES, HydratorPlusPlusPreviewStore, HydratorPlusPlusPreviewActions, myPipelineApi, $state, myAlertOnValium) {
    this.uuid = uuid;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.previewStore = HydratorPlusPlusPreviewStore;
    this.previewActions = HydratorPlusPlusPreviewActions;
    this.myPipelineApi = myPipelineApi;
    this.$state = $state;
    this.myAlertOnValium = myAlertOnValium;

    this.backpressure = this.store.getBackpressure();
    this.numExecutors = this.store.getNumExecutors();
    this.instrumentation = this.store.getInstrumentation();
    this.stageLogging = this.store.getStageLogging();
    this.checkpointing = this.store.getCheckpointing();
    this.batchInterval = this.store.getBatchInterval();
    this.batchIntervalTime = this.batchInterval.substring(0, this.batchInterval.length - 1);
    this.batchIntervalUnit = this.batchInterval.charAt(this.batchInterval.length - 1);
    let previewStoreState = this.previewStore.getState().preview;
    this.timeoutInMinutes = previewStoreState.timeoutInMinutes;

    this.startingPipeline = false;
    this.updatingPipeline = false;
    this.numExecutorsOptions = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'];

    let batchIntervalTimeOptions = [];
    for (let i = 1; i <= 60; i++) {
      batchIntervalTimeOptions.push(i.toString());
    }

    this.batchIntervalTimeOptions = batchIntervalTimeOptions;
    this.batchIntervalUnits = {
      's': 'Seconds',
      'm': 'Minutes'
    };

    this.numberConfig = {
      'widget-attributes': {
        min: 0,
        max: 15,
        default: this.timeoutInMinutes,
        showErrorMessage: false,
        convertToInteger: true
      }
    };

    this.customEngineConfig = {
      'pairs': HydratorPlusPlusHydratorService.convertMapToKeyValuePairs(this.store.getCustomConfigForDisplay())
    };

    if (this.customEngineConfig.pairs.length === 0 && !this.isDeployed) {
      this.customEngineConfig.pairs.push({
        key: '',
        value: '',
        uniqueId: 'id-' + this.uuid.v4()
      });
    }

    this.activeTab = 'runtimeArgs';
    // studio config, but not in preview mode
    if (!this.isDeployed && !this.showPreviewConfig) {
      this.activeTab = 'pipelineConfig';
    }

    this.enablePipelineUpdate = false;
    this.runtimeArguments = this.checkForReset(this.runtimeArguments);
    this.onRuntimeArgumentsChange = this.onRuntimeArgumentsChange.bind(this);
    this.onCustomEngineConfigChange = this.onCustomEngineConfigChange.bind(this);
    this.getResettedRuntimeArgument = this.getResettedRuntimeArgument.bind(this);
    this.onDriverMemoryChange = this.onDriverMemoryChange.bind(this);
    this.onDriverCoreChange = this.onDriverCoreChange.bind(this);
    this.onExecutorCoreChange = this.onExecutorCoreChange.bind(this);
    this.onExecutorMemoryChange = this.onExecutorMemoryChange.bind(this);
    this.onClientCoreChange = this.onClientCoreChange.bind(this);
    this.onClientMemoryChange = this.onClientMemoryChange.bind(this);
    this.driverResources = {
      memoryMB: this.store.getDriverMemoryMB(),
      virtualCores: this.store.getDriverVirtualCores()
    };
    this.executorResources = {
      memoryMB: this.store.getMemoryMB(),
      virtualCores: this.store.getVirtualCores()
    };
    this.clientResources = {
      memoryMB: this.store.getClientMemoryMB(),
      virtualCores: this.store.getClientVirtualCores()
    };

    this.containsMacros = HydratorPlusPlusHydratorService.runtimeArgsContainsMacros(this.runtimeArguments);
  }

  onRuntimeArgumentsChange(newRuntimeArguments) {
    this.runtimeArguments = this.checkForReset(newRuntimeArguments);
  }

  onCustomEngineConfigChange(newCustomConfig) {
    this.customEngineConfig = newCustomConfig;
  }

  checkForReset(runtimeArguments) {
    let runtimeArgumentsPairs = runtimeArguments.pairs;
    for (let i = 0; i < runtimeArgumentsPairs.length; i++) {
      if (runtimeArgumentsPairs[i].notDeletable) {
        if (runtimeArgumentsPairs[i].provided) {
          runtimeArgumentsPairs[i].showReset = false;
        } else {
          let runtimeArgKey = runtimeArgumentsPairs[i].key;
          if (this.resolvedMacros.hasOwnProperty(runtimeArgKey)) {
            if (this.resolvedMacros[runtimeArgKey] !== runtimeArgumentsPairs[i].value) {
              runtimeArgumentsPairs[i].showReset = true;
            } else {
              runtimeArgumentsPairs[i].showReset = false;
            }
          }
        }
      }
    }
    return runtimeArguments;
  }

  getResettedRuntimeArgument(index) {
    let runtimeArgKey = this.runtimeArguments.pairs[index].key;
    this.runtimeArguments.pairs[index].value = this.resolvedMacros[runtimeArgKey];
    window.CaskCommon.KeyValueStore.dispatch({
      type: window.CaskCommon.KeyValueStoreActions.onUpdate,
      payload: {pairs: this.runtimeArguments.pairs}
    });
  }

  applyConfig() {
    this.applyRuntimeArguments();
    if (!this.isDeployed) {
      this.store.setBackpressure(this.backpressure);
      this.store.setNumExecutors(this.numExecutors);
      this.store.setCustomConfig(this.HydratorPlusPlusHydratorService.convertKeyValuePairsToMap(this.customEngineConfig));
      this.store.setInstrumentation(this.instrumentation);
      this.store.setStageLogging(this.stageLogging);
      this.store.setCheckpointing(this.checkpointing);
      this.store.setBatchInterval(this.batchIntervalTime + this.batchIntervalUnit);
      this.store.setClientVirtualCores(this.clientResources.virtualCores);
      this.store.setClientMemoryMB(this.clientResources.memoryMB);
      this.store.setDriverVirtualCores(this.driverResources.virtualCores);
      this.store.setDriverMemoryMB(this.driverResources.memoryMB);
      this.store.setMemoryMB(this.executorResources.memoryMB);
      this.store.setVirtualCores(this.executorResources.virtualCores);
      this.previewStore.dispatch(
        this.previewActions.setTimeoutInMinutes(this.timeoutInMinutes)
      );
    }
  }

  applyAndRunPipeline() {
    const applyAndRun = () => {
      this.startingPipeline = false;
      this.applyConfig();
      this.runPipeline();
    };

    this.startingPipeline = true;
    if (this.enablePipelineUpdate) {
      this.updatePipeline(false)
        .then(
          applyAndRun.bind(this),
          (err) => {
            this.startingPipeline = false;
            this.myAlertOnValium.show({
              type: 'danger',
              content: typeof err === 'object' ? JSON.stringify(err) : 'Updating pipeline failed: '+ err
            });
          }
        );
    } else {
      applyAndRun.call(this);
    }
  }

  applyAndClose() {
    this.applyConfig();
    this.onClose();
  }

  buttonsAreDisabled() {
    let runtimeArgsMissingValues = false;
    let customConfigMissingValues = false;

    if (this.isDeployed || this.showPreviewConfig) {
      runtimeArgsMissingValues = this.HydratorPlusPlusHydratorService.keyValuePairsHaveMissingValues(this.runtimeArguments);
    }
    customConfigMissingValues = this.HydratorPlusPlusHydratorService.keyValuePairsHaveMissingValues(this.customEngineConfig);
    return runtimeArgsMissingValues || customConfigMissingValues;
  }

  onDriverMemoryChange(value) {
    this.driverResources.memoryMB = value;
    this.updatePipelineEditStatus();
  }
  onDriverCoreChange(value) {
    this.driverResources.virtualCores = value;
    this.updatePipelineEditStatus();
  }
  onExecutorCoreChange(value) {
    this.executorResources.virtualCores = value;
    this.updatePipelineEditStatus();
  }
  onExecutorMemoryChange(value) {
    this.executorResources.memoryMB = value;
    this.updatePipelineEditStatus();
  }
  onClientCoreChange(value) {
    this.clientResources.virtualCores = value;
    this.updatePipelineEditStatus();
  }
  onClientMemoryChange(value) {
    this.clientResources.memoryMB = value;
    this.updatePipelineEditStatus();
  }

  onCheckPointingChange() {
    this.checkpointing = !this.checkpointing;
    this.updatePipelineEditStatus();
  }

  onInstrumentationChange() {
    this.instrumentation = !this.instrumentation;
    this.updatePipelineEditStatus();
  }

  onStageLoggingChange() {
    this.stageLogging = !this.stageLogging;
    this.updatePipelineEditStatus();
  }

  getUpdatedPipelineConfig() {

    let pipelineconfig = _.cloneDeep(this.store.getCloneConfig());
    delete pipelineconfig.__ui__;
    if (this.instrumentation) {
      pipelineconfig.config.stageLoggingEnabled = this.instrumentation;
    }
    pipelineconfig.config.batchInterval = this.batchIntervalTime + this.batchIntervalUnit;
    pipelineconfig.config.resources = this.executorResources;
    pipelineconfig.config.driverResources = this.driverResources;
    pipelineconfig.config.clientResources = this.clientResources;
    pipelineconfig.config.disableCheckpoints = this.checkpointing;
    pipelineconfig.config.processTimingEnabled = this.instrumentation;
    pipelineconfig.config.stageLoggingEnabled = this.stageLogging;
    return pipelineconfig;
  }

  updatePipelineEditStatus() {
    const isResourcesEqual = (oldvalue, newvalue) => {
      return oldvalue.memoryMB === newvalue.memoryMB && oldvalue.virtualCores === newvalue.virtualCores;
    };
    let oldConfig = this.store.getCloneConfig();
    let updatedConfig = this.getUpdatedPipelineConfig();
    let isResourceModified = !isResourcesEqual(oldConfig.config.resources, updatedConfig.config.resources);
    let isDriverResourceModidified = !isResourcesEqual(oldConfig.config.driverResources, updatedConfig.config.driverResources);
    let isClientResourceModified = !isResourcesEqual(oldConfig.config.clientResources, updatedConfig.config.clientResources);

    let isDisableCheckpointModified = oldConfig.config.disableCheckpoints !== updatedConfig.config.disableCheckpoints;
    let isProcessTimingModified = oldConfig.config.processTimingEnabled !== updatedConfig.config.processTimingEnabled;
    let isStageLoggingModified = oldConfig.config.stageLoggingEnabled !== updatedConfig.config.stageLoggingEnabled;
    let isBatchIntervalModified = oldConfig.config.batchInterval !== updatedConfig.config.batchInterval;

    this.enablePipelineUpdate = (
      isResourceModified ||
      isDriverResourceModidified ||
      isClientResourceModified ||
      isDisableCheckpointModified ||
      isProcessTimingModified ||
      isStageLoggingModified ||
      isBatchIntervalModified
    );
  }
  updatePipeline(updatingPipeline = true) {
    let pipelineConfig = this.getUpdatedPipelineConfig();
    this.updatingPipeline = updatingPipeline;
    return this.myPipelineApi
      .save(
        {
          namespace: this.$state.params.namespace,
          pipeline: pipelineConfig.name
        },
        pipelineConfig
      )
      .$promise
      .then(
        () => {
          return this.$state.reload().then(() => this.updatingPipeline = false);
        },
        (err) => {
          this.updatingPipeline = false;
          this.myAlertOnValium.show({
            type: 'danger',
            content: typeof err === 'object' ? JSON.stringify(err) : 'Updating pipeline failed: ' + err
          });
        }
      );
  }
}

MyRealtimePipelineConfigCtrl.$inject = ['uuid', 'HydratorPlusPlusHydratorService', 'HYDRATOR_DEFAULT_VALUES', 'HydratorPlusPlusPreviewStore', 'HydratorPlusPlusPreviewActions', 'myPipelineApi', '$state',
'myAlertOnValium'];
angular.module(PKG.name + '.commons')
  .directive('keyValuePairs', function(reactDirective) {
    return reactDirective(window.CaskCommon.KeyValuePairs);
  })
  .controller('MyRealtimePipelineConfigCtrl', MyRealtimePipelineConfigCtrl);
