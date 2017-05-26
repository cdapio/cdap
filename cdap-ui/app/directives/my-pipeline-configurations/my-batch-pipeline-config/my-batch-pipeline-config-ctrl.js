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

class MyBatchPipelineConfigCtrl {
  constructor(uuid, HydratorPlusPlusHydratorService, HYDRATOR_DEFAULT_VALUES) {
    this.uuid = uuid;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;

    this.engine = this.store.getEngine();
    this.engineForDisplay = this.engine === 'mapreduce' ? 'MapReduce' : 'Apache Spark Streaming';
    this.instrumentation = this.store.getInstrumentation();
    this.stageLogging = this.store.getStageLogging();
    this.numRecordsPreview = this.store.getNumRecordsPreview();
    this.numberConfig = {
      'widget-attributes': {
        min: 0,
        default: HYDRATOR_DEFAULT_VALUES.numOfRecordsPreview,
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

  onEngineChange() {
    this.engineForDisplay = this.engine === 'mapreduce' ? 'MapReduce' : 'Apache Spark Streaming';
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
      this.store.setEngine(this.engine);
      this.store.setCustomConfig(this.HydratorPlusPlusHydratorService.convertKeyValuePairsToMap(this.customEngineConfig));
      this.store.setInstrumentation(this.instrumentation);
      this.store.setStageLogging(this.stageLogging);
      this.store.setNumRecordsPreview(this.numRecordsPreview);
    }
  }

  applyAndClose() {
    this.applyConfig();
    this.onClose();
  }

  applyAndRunPipeline() {
    this.applyConfig();
    this.runPipeline();
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

MyBatchPipelineConfigCtrl.$inject = ['uuid', 'HydratorPlusPlusHydratorService', 'HYDRATOR_DEFAULT_VALUES'];
angular.module(PKG.name + '.commons')
  .directive('keyValuePairs', function(reactDirective) {
    return reactDirective(window.CaskCommon.KeyValuePairs);
  })

  .controller('MyBatchPipelineConfigCtrl', MyBatchPipelineConfigCtrl);
