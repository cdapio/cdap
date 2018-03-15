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
import {setRunButtonLoading, setRunError, setScheduleButtonLoading, setScheduleError, fetchScheduleStatus} from 'components/PipelineDetails/store/ActionCreator';
import KeyValueStore from 'components/KeyValuePairs/KeyValueStore';
import KeyValueStoreActions from 'components/KeyValuePairs/KeyValueStoreActions';
import {convertKeyValuePairsObjToMap} from 'components/KeyValuePairs/KeyValueStoreActions';
import {GLOBALS} from 'services/global-constants';
import {MyPipelineApi} from 'api/pipeline';
import {MyProgramApi} from 'api/program';
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
  KeyValueStore.dispatch({
    type: KeyValueStoreActions.onUpdate,
    payload: { pairs: savedRuntimeArgs.pairs }
  });
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
    payload: { runtimeArgs: cloneDeep(savedRuntimeArgs) }
  });
};

const updateKeyValueStore = () => {
  let runtimeArgsPairs = PipelineConfigurationsStore.getState().runtimeArgs.pairs;
  KeyValueStore.dispatch({
    type: KeyValueStoreActions.onUpdate,
    payload: { pairs: runtimeArgsPairs }
  });
};

const getMacrosResolvedByPrefs = (resolvedPrefs = {}, macrosMap = {}) => {
  let resolvedMacros = {...macrosMap};
  for (let pref in resolvedPrefs) {
    if (resolvedPrefs.hasOwnProperty(pref) && resolvedMacros.hasOwnProperty(pref)) {
      resolvedMacros[pref] = resolvedPrefs[pref];
    }
  }
  return resolvedMacros;
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

const runPipeline = () => {
  setRunButtonLoading(true);
  let { name, artifact } = PipelineDetailStore.getState();
  let { runtimeArgs  } = PipelineConfigurationsStore.getState();

  let params = {
    namespace: getCurrentNamespace(),
    appId: name,
    programType: GLOBALS.programType[artifact.name],
    programId: GLOBALS.programId[artifact.name],
    action: 'start'
  };
  runtimeArgs = convertKeyValuePairsObjToMap(runtimeArgs);
  MyProgramApi.action(params, runtimeArgs)
  .subscribe(
    () => {},
    (err) => {
    setRunButtonLoading(false);
    setRunError(err.response || err);
  });
};

const schedulePipeline = () => {
  scheduleOrSuspendPipeline(MyPipelineApi.schedule);
};

const suspendSchedule = () => {
  scheduleOrSuspendPipeline(MyPipelineApi.suspend);
};

const scheduleOrSuspendPipeline = (scheduleApi) => {
  setScheduleButtonLoading(true);
  let { name } = PipelineDetailStore.getState();

  let params = {
    namespace: getCurrentNamespace(),
    appId: name,
    scheduleId: GLOBALS.defaultScheduleId
  };
  scheduleApi(params)
  .subscribe(() => {
    setScheduleButtonLoading(false);
    fetchScheduleStatus(params);
  }, (err) => {
    setScheduleButtonLoading(false);
    setScheduleError(err.response || err);
  });
};

export {
  applyRuntimeArgs,
  revertRuntimeArgsToSavedValues,
  updateKeyValueStore,
  getMacrosResolvedByPrefs,
  updatePipelineEditStatus,
  updatePipeline,
  runPipeline,
  schedulePipeline,
  suspendSchedule
};
