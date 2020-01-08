/*
* Copyright © 2019 Cask Data, Inc.
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

class MySqlPipelineConfigCtrl {
  constructor(uuid, HydratorPlusPlusHydratorService, HydratorPlusPlusPreviewStore, HydratorPlusPlusPreviewActions, myPipelineApi, $state, myAlertOnValium) {
    this.uuid = uuid;
    this.HydratorPlusPlusHydratorService = HydratorPlusPlusHydratorService;
    this.previewStore = HydratorPlusPlusPreviewStore;
    this.previewActions = HydratorPlusPlusPreviewActions;
    this.myPipelineApi = myPipelineApi;
    this.$state = $state;
    this.myAlertOnValium = myAlertOnValium;

    this.serviceAccountPath = this.store.getServiceAccountPath();
    this.instrumentation = this.store.getInstrumentation();
    this.stageLogging = this.store.getStageLogging();

    this.startingPipeline = false;
    this.updatingPipeline = false;

    this.activeTab = 'runtimeArgs';
    // studio config, but not in preview mode
    if (!this.isDeployed && !this.showPreviewConfig) {
      this.activeTab = 'pipelineConfig';
    }

    this.enablePipelineUpdate = false;
    this.onDriverMemoryChange = this.onDriverMemoryChange.bind(this);
    this.onDriverCoreChange = this.onDriverCoreChange.bind(this);
    this.onExecutorCoreChange = this.onExecutorCoreChange.bind(this);
    this.onExecutorMemoryChange = this.onExecutorMemoryChange.bind(this);
    this.onClientCoreChange = this.onClientCoreChange.bind(this);
    this.onClientMemoryChange = this.onClientMemoryChange.bind(this);
    this.onServiceAccountChange = this.onServiceAccountChange.bind(this);
    this.onInstrumentationChange = this.onInstrumentationChange.bind(this);
    this.onStageLoggingChange = this.onStageLoggingChange.bind(this);
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

  applyConfig() {
    this.applyRuntimeArguments();
    this.store.setClientVirtualCores(this.clientResources.virtualCores);
    this.store.setClientMemoryMB(this.clientResources.memoryMB);
    this.store.setDriverVirtualCores(this.driverResources.virtualCores);
    this.store.setDriverMemoryMB(this.driverResources.memoryMB);
    this.store.setMemoryMB(this.executorResources.memoryMB);
    this.store.setVirtualCores(this.executorResources.virtualCores);
    this.store.setInstrumentation(this.instrumentation);
    this.store.setStageLogging(this.stageLogging);
    this.store.setServiceAccountPath(this.serviceAccountPath);
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

  updateAndClose() {
    this.updatePipeline()
    .then(() => {
      this.applyConfig();
      this.onClose();
    });
  }

  buttonsAreDisabled() {
    let runtimeArgsMissingValues = false;

    if (this.isDeployed || this.showPreviewConfig) {
      runtimeArgsMissingValues = this.HydratorPlusPlusHydratorService.keyValuePairsHaveMissingValues(this.runtimeArguments);
    }
    return runtimeArgsMissingValues;
  }

  onServiceAccountChange(value) {
    this.serviceAccountPath = value;
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

  getUpdatedPipelineConfig() {
    let pipelineconfig = _.cloneDeep(this.store.getCloneConfig());
    delete pipelineconfig.__ui__;
    if (this.instrumentation) {
      pipelineconfig.config.stageLoggingEnabled = this.instrumentation;
    }
    pipelineconfig.config.resources = this.executorResources;
    pipelineconfig.config.driverResources = this.driverResources;
    pipelineconfig.config.clientResources = this.clientResources;
    pipelineconfig.config.serviceAccountPath = this.serviceAccountPath;
    pipelineconfig.config.processTimingEnabled = this.instrumentation;
    pipelineconfig.config.stageLoggingEnabled = this.stageLogging;

    // Have to do this, because unlike others we aren't actually directly modifying pipelineconfig.config.properties
    pipelineconfig.config.properties = this.store.getProperties();

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
    let isServiceAccountPathModified = oldConfig.config.serviceAccountPath !== updatedConfig.config.serviceAccountPath;
    let isProcessTimingModified = oldConfig.config.processTimingEnabled !== updatedConfig.config.processTimingEnabled;
    let isStageLoggingModified = oldConfig.config.stageLoggingEnabled !== updatedConfig.config.stageLoggingEnabled;

    // Pipeline update is only necessary in Detail view (i.e. after pipeline has been deployed)
    this.enablePipelineUpdate = this.isDeployed && (
      isResourceModified ||
      isDriverResourceModidified ||
      isClientResourceModified ||
      isServiceAccountPathModified ||
      isProcessTimingModified ||
      isStageLoggingModified
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
          this.updatingPipeline = false;
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

angular.module(PKG.name + '.commons')
  .controller('MySqlPipelineConfigCtrl', MySqlPipelineConfigCtrl);
