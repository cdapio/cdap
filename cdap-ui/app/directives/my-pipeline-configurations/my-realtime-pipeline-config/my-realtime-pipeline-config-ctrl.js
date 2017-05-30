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
  constructor(uuid, HydratorPlusPlusHydratorService, HYDRATOR_DEFAULT_VALUES, HydratorPlusPlusPreviewStore, HydratorPlusPlusPreviewActions) {
    this.uuid = uuid;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.previewStore = HydratorPlusPlusPreviewStore;
    this.previewActions = HydratorPlusPlusPreviewActions;

    this.backpressure = this.store.getBackpressure();
    this.numExecutors = this.store.getNumExecutors();
    this.instrumentation = this.store.getInstrumentation();
    this.stageLogging = this.store.getStageLogging();
    this.checkpointing = this.store.getCheckpointing();
    this.gracefulStop = this.store.getGracefulStop();
    this.batchInterval = this.store.getBatchInterval();
    this.batchIntervalTime = this.batchInterval.substring(0, this.batchInterval.length - 1);
    this.batchIntervalUnit = this.batchInterval.charAt(this.batchInterval.length - 1);
    let previewStoreState = this.previewStore.getState().preview;
    this.timeoutInMinutes = previewStoreState.timeoutInMinutes;

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

    if (this.customEngineConfig.pairs.length === 0 && !this.isDisabled) {
      this.customEngineConfig.pairs.push({
        key: '',
        value: '',
        uniqueId: 'id-' + this.uuid.v4()
      });
    }

    this.activeTab = 'runtimeArgs';
    // studio config, but not in preview mode
    if (!this.isDisabled && !this.showPreviewConfig) {
      this.activeTab = 'pipelineConfig';
    }

    this.runtimeArguments = this.checkForReset(this.runtimeArguments);
    this.onRuntimeArgumentsChange = this.onRuntimeArgumentsChange.bind(this);
    this.onCustomEngineConfigChange = this.onCustomEngineConfigChange.bind(this);
    this.getResettedRuntimeArgument = this.getResettedRuntimeArgument.bind(this);
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
    if (!this.isDisabled) {
      this.store.setBackpressure(this.backpressure);
      this.store.setNumExecutors(this.numExecutors);
      this.store.setCustomConfig(this.HydratorPlusPlusHydratorService.convertKeyValuePairsToMap(this.customEngineConfig));
      this.store.setInstrumentation(this.instrumentation);
      this.store.setStageLogging(this.stageLogging);
      this.store.setCheckpointing(this.checkpointing);
      this.store.setGracefulStop(this.gracefulStop);
      this.store.setBatchInterval(this.batchIntervalTime + this.batchIntervalUnit);
      this.previewStore.dispatch(
        this.previewActions.setTimeoutInMinutes(this.timeoutInMinutes)
      );
    }
  }

  applyAndRunPipeline() {
    this.applyConfig();
    this.runPipeline();
  }

  applyAndClose() {
    this.applyConfig();
    this.onClose();
  }

  buttonsAreDisabled() {
    let runtimeArgsMissingValues = false;
    let customConfigMissingValues = false;

    if (this.isDisabled || this.showPreviewConfig) {
      runtimeArgsMissingValues = this.HydratorPlusPlusHydratorService.keyValuePairsHaveMissingValues(this.runtimeArguments);
    }
    customConfigMissingValues = this.HydratorPlusPlusHydratorService.keyValuePairsHaveMissingValues(this.customEngineConfig);
    return runtimeArgsMissingValues || customConfigMissingValues;
  }
}

MyRealtimePipelineConfigCtrl.$inject = ['uuid', 'HydratorPlusPlusHydratorService', 'HYDRATOR_DEFAULT_VALUES', 'HydratorPlusPlusPreviewStore', 'HydratorPlusPlusPreviewActions'];
angular.module(PKG.name + '.commons')
  .directive('keyValuePairs', function(reactDirective) {
    return reactDirective(window.CaskCommon.KeyValuePairs);
  })
  .controller('MyRealtimePipelineConfigCtrl', MyRealtimePipelineConfigCtrl);
