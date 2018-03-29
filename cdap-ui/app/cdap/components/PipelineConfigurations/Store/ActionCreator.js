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
import KeyValueStore, {getDefaultKeyValuePair} from 'components/KeyValuePairs/KeyValueStore';
import KeyValueStoreActions, {convertKeyValuePairsObjToMap} from 'components/KeyValuePairs/KeyValueStoreActions';
import {GLOBALS} from 'services/global-constants';
import {MyPipelineApi} from 'api/pipeline';
import {MyProgramApi} from 'api/program';
import {getCurrentNamespace} from 'services/NamespaceStore';
import cloneDeep from 'lodash/cloneDeep';
import { MyPreferenceApi } from 'api/preference';
import {Observable} from 'rxjs/Observable';
import difference from 'lodash/difference';
import {PROFILE_NAME_PREFERENCE_PROPERTY} from 'components/PipelineConfigurations/ConfigurationsContent/ComputeTabContent/ProfilesListView';

const RUNTIME_ARGS_TO_SKIP_DURING_DISPLAY = [
  PROFILE_NAME_PREFERENCE_PROPERTY,
  'logical.start.time'
];

const applyRuntimeArgs = () => {
  let runtimeArgs = PipelineConfigurationsStore.getState().runtimeArgs;
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.SET_SAVED_RUNTIME_ARGS,
    payload: { savedRuntimeArgs: cloneDeep(runtimeArgs) }
  });
};

// Filter certain preferences from being shown in the run time arguments 
// They are being represented in other places (like selected compute profile).
const getFilteredRuntimeArgs = (hideProvided) => {
  let {runtimeArgs, resolvedMacros} = PipelineConfigurationsStore.getState();
  let modifiedRuntimeArgs = {};
  let pairs = [...runtimeArgs.pairs];
  pairs = pairs
    .filter(pair => (RUNTIME_ARGS_TO_SKIP_DURING_DISPLAY.indexOf(pair.key) === -1))
    .map(pair => {
      if (pair.key in resolvedMacros) {
        return {
          notDeletable: true,
          provided: hideProvided ? null : pair.provided || false,
          ...pair
        };
      }
      return {
        ...pair,
        // This is needed because KeyValuePair will render a checkbox only if the provided is a boolean.
        provided: null
      };
    });
  if (!pairs.length) {
    pairs.push(getDefaultKeyValuePair());
  }
  modifiedRuntimeArgs.pairs = pairs;
  return modifiedRuntimeArgs;
};

// While adding runtime argument make sure to include the excluded preferences
const updateRunTimeArgs = (rtArgs) => {
  let {runtimeArgs} = PipelineConfigurationsStore.getState();
  let modifiedRuntimeArgs = {};
  let excludedPairs = [...runtimeArgs.pairs];
  const preferencesToFilter = [PROFILE_NAME_PREFERENCE_PROPERTY];
  excludedPairs = excludedPairs.filter(pair => preferencesToFilter.indexOf(pair.key) !== -1);
  modifiedRuntimeArgs.pairs = rtArgs.pairs.concat(excludedPairs);
  updatePipelineEditStatus();
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
    payload: {
      runtimeArgs: modifiedRuntimeArgs
    }
  });
};

const revertConfigsToSavedValues = () => {
  let savedRuntimeArgs = PipelineConfigurationsStore.getState().savedRuntimeArgs;
  KeyValueStore.dispatch({
    type: KeyValueStoreActions.onUpdate,
    payload: { pairs: savedRuntimeArgs.pairs }
  });
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.INITIALIZE_CONFIG,
    payload: {...PipelineDetailStore.getState().config}
  });
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
    payload: { runtimeArgs: cloneDeep(savedRuntimeArgs) }
  });
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.SET_MODELESS_OPEN_STATUS,
    payload: { open: false }
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

const updatePreferences = () => {
  let {runtimeArgs} = PipelineConfigurationsStore.getState();
  let appId = PipelineDetailStore.getState().name;
  let prefObj = convertKeyValuePairsObjToMap(runtimeArgs);

  if (!Object.keys(prefObj).length) {
    return Observable.create((observer) => {
      observer.next();
    });
  }
  return MyPreferenceApi.setAppPreferences({
    namespace: getCurrentNamespace(),
    appId
  }, prefObj);
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
  let isRunTimeArgsModified = difference(updatedConfig.runtimeArgs, updatedConfig.savedRuntimeArgs);

  let isModified = (
    isResourcesModified ||
    isDriverResourcesModidified ||
    isInstrumentationModified ||
    isStageLoggingModified ||
    isCustomEngineConfigModified ||
    isRunTimeArgsModified
  );

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

  let params = {
    namespace: getCurrentNamespace(),
    appId: name,
    programType: GLOBALS.programType[artifact.name],
    programId: GLOBALS.programId[artifact.name],
    action: 'start'
  };
  MyProgramApi.action(params)
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

const reset = () => {
  PipelineConfigurationsStore.dispatch({
    type: PipelineConfigurationsActions.RESET
  });
};

export {
  applyRuntimeArgs,
  getFilteredRuntimeArgs,
  updateRunTimeArgs,
  revertConfigsToSavedValues,
  updateKeyValueStore,
  getMacrosResolvedByPrefs,
  updatePipelineEditStatus,
  updatePipeline,
  updatePreferences,
  runPipeline,
  schedulePipeline,
  suspendSchedule,
  reset
};
