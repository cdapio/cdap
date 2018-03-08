/*
 * Copyright © 2018 Cask Data, Inc.
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

import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import PipelineDetailStore, {ACTIONS as PipelineDetailActions} from 'components/PipelineDetails/store';
import {GLOBALS} from 'services/global-constants';
import {MyPipelineApi} from 'api/pipeline';
import {getCurrentNamespace} from 'services/NamespaceStore';
import cloneDeep from 'lodash/cloneDeep';

const applyRuntimeArgs = () => {
  let runtimeArgs = PipelineConfigurationsStore.getState().runtimeArgs;
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.SET_SAVED_RUNTIME_ARGS,
    payload: { savedRuntimeArgs: cloneDeep(runtimeArgs) }
  });
};

const revertRuntimeArgsToSavedValues = () => {
  let savedRuntimeArgs = PipelineConfigurationsStore.getState().savedRuntimeArgs;
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
    payload: { runtimeArgs: cloneDeep(savedRuntimeArgs) }
  });
};

const updatePipelineEditStatus = () => {
  const isResourcesEqual = (oldvalue, newvalue) => {
    return oldvalue.memoryMB === newvalue.memoryMB && oldvalue.virtualCores === newvalue.virtualCores;
  };

  let oldConfig = PipelineDetailStore.getState().config;
  let updatedConfig = PipelineConfigurationsStore.getState();

  // These are the values that user can modify in Detail view
  let isResourcesModified = !isResourcesEqual(oldConfig.resources, updatedConfig.resources);
  let isDriverResourcesModidified = !isResourcesEqual(oldConfig.driverResources, updatedConfig.driverResources);
  let isInstrumentationModified = oldConfig.processTimingEnabled !== updatedConfig.processTimingEnabled;
  let isStageLoggingModified = oldConfig.stageLoggingEnabled !== updatedConfig.stageLoggingEnabled;
  let isCustomEngineConfigModified = oldConfig.properties !== updatedConfig.properties;

  let isModified = isResourcesModified || isDriverResourcesModidified || isInstrumentationModified || isStageLoggingModified || isCustomEngineConfigModified;

  if (PipelineDetailStore.getState().artifact.name === GLOBALS.etlDataStreams) {
    let isClientResourcesModified = !isResourcesEqual(oldConfig.clientResources, updatedConfig.clientResources);
    let isBatchIntervalModified = oldConfig.batchInterval !== updatedConfig.batchInterval;
    let isCheckpointModified = oldConfig.disableCheckpoints !== updatedConfig.disableCheckpoints;
    isModified = isModified || isClientResourcesModified || isBatchIntervalModified || isCheckpointModified;
  }

  if (isModified !== PipelineConfigurationsStore.getState().pipelineEdited) {
    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.SET_PIPELINE_EDIT_STATUS,
      payload: { pipelineEdited: isModified }
    });
  }
};

const updatePipeline = () => {
  let detailStoreState = PipelineDetailStore.getState();
  let { name, description, artifact, principal } = detailStoreState;
  let { stages, connections, comments } = detailStoreState.config;

  let {
    batchInterval,
    engine,
    resources,
    driverResources,
    clientResources,
    postActions,
    properties,
    processTimingEnabled,
    stageLoggingEnabled,
    disableCheckpoints,
    stopGracefully,
    schedule,
    maxConcurrentRuns,
  } = PipelineConfigurationsStore.getState();

  let commonConfig = {
    stages,
    connections,
    comments,
    resources,
    driverResources,
    postActions,
    properties,
    processTimingEnabled,
    stageLoggingEnabled
  };

  let batchOnlyConfig = {
    engine,
    schedule,
    maxConcurrentRuns
  };

  let realtimeOnlyConfig = {
    batchInterval,
    clientResources,
    disableCheckpoints,
    stopGracefully
  };

  let config;
  if (artifact.name === GLOBALS.etlDataPipeline) {
    config = {...commonConfig, ...batchOnlyConfig};
  } else {
    config = {...commonConfig, ...realtimeOnlyConfig};
  }

  let publishObservable = MyPipelineApi.publish({
    namespace: getCurrentNamespace(),
    appId: name
  }, {
    name,
    description,
    artifact,
    config,
    principal
  });

  publishObservable.subscribe(() => {
    PipelineDetailStore.dispatch({
      type: PipelineDetailActions.SET_CONFIG,
      payload: { config }
    });
  });

  return publishObservable;
};

export {
  applyRuntimeArgs,
  revertRuntimeArgsToSavedValues,
  updatePipelineEditStatus,
  updatePipeline
};
