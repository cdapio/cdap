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

import {MyPipelineApi} from 'api/pipeline';
import PipelineDetailStore, {ACTIONS} from 'components/PipelineDetails/store';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {MyPreferenceApi} from 'api/preference';
import {objectQuery} from 'services/helpers';
import {getMacrosResolvedByPrefs} from 'components/PipelineConfigurations/Store/ActionCreator';
import PipelineConfigurationsStore, {ACTIONS as PipelineConfigurationsActions} from 'components/PipelineConfigurations/Store';
import uuidV4 from 'uuid/v4';
import uniqBy from 'lodash/uniqBy';
import {PROFILE_NAME_PREFERENCE_PROPERTY, PROFILE_PROPERTIES_PREFERENCE} from 'components/PipelineDetails/ProfilesListView';

const init = (pipeline) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.INITIALIZE_PIPELINE_DETAILS,
    payload: { pipeline }
  });
};

const setOptionalProperty = (key, value) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_OPTIONAL_PROPERTY,
    payload: { key, value }
  });
};

const setSchedule = (schedule) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_SCHEDULE,
    payload: { schedule }
  });
};

const fetchScheduleStatus = (params) => {
  MyPipelineApi
    .getScheduleStatus(params)
    .subscribe(schedule => {
      PipelineDetailStore.dispatch({
        type: ACTIONS.SET_SCHEDULE_STATUS,
        payload: {
          scheduleStatus: schedule.status
        }
      });
    }, (err) => {
      console.log(err);
    });
};

const setEngine = (schedule) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_ENGINE,
    payload: { schedule }
  });
};

const setBatchInterval = (batchInterval) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_BATCH_INTERVAL,
    payload: { batchInterval }
  });
};

const setMemoryMB = (memoryMB) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_MEMORY_MB,
    payload: { memoryMB }
  });
};

const setVirtualCores = (virtualCores) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_MEMORY_VIRTUAL_CORES,
    payload: { virtualCores }
  });
};

const setDriverMemoryMB = (memoryMB) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_DRIVER_MEMORY_MB,
    payload: { memoryMB }
  });
};

const setDriverVirtualCores = (virtualCores) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_DRIVER_VIRTUAL_CORES,
    payload: { virtualCores }
  });
};

const setClientMemoryMB = (memoryMB) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_CLIENT_MEMORY_MB,
    payload: { memoryMB }
  });
};

const setClientVirtualCores = (virtualCores) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_CLIENT_VIRTUAL_CORES,
    payload: { virtualCores }
  });
};

const setBackpressure = (backpressure) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_BACKPRESSURE,
    payload: { backpressure }
  });
};

const setCustomConfig = (customConfig) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_CUSTOM_CONFIG,
    payload: { customConfig }
  });
};

const setNumExecutors = (numExecutors) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_NUM_EXECUTORS,
    payload: { numExecutors }
  });
};

const setInstrumentation = (instrumentation) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_INSTRUMENTATION,
    payload: { instrumentation }
  });
};

const setStageLogging = (stageLogging) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_STAGE_LOGGING,
    payload: { stageLogging }
  });
};

const setCheckpointing = (checkpointing) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_CHECKPOINTING,
    payload: { checkpointing }
  });
};

const setNumRecordsPreview = (numRecordsPreview) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_NUM_RECORDS_PREVIEW,
    payload: { numRecordsPreview }
  });
};

const setMaxConcurrentRuns = (maxConcurrentRuns) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_MAX_CONCURRENT_RUNS,
    payload: { maxConcurrentRuns }
  });
};

const setCurrentRunId = (runId) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_CURRENT_RUN_ID,
    payload: { runId }
  });
};

const getRuns = (params) => {
  let runsFetch = MyPipelineApi.getRuns(params);
  runsFetch.subscribe(runs => {
    PipelineDetailStore.dispatch({
      type: ACTIONS.SET_RUNS,
      payload: { runs }
    });
  }, (err) => {
    console.log(err);
  });
  return runsFetch;
};

const pollRuns = (params) => {
  return MyPipelineApi
    .pollRuns(params)
    .subscribe(runs => {
      // When there are new runs, always set current run to most recent run
      let currentRuns = PipelineDetailStore.getState().runs;

      if (runs.length && (runs.length > currentRuns.length || runs[0].runid !== currentRuns[0].runid || runs[0].status !== currentRuns[0].status)) {
        PipelineDetailStore.dispatch({
          type: ACTIONS.SET_CURRENT_RUN_ID,
          payload: { runId: runs[0].runid }
        });
      }

      PipelineDetailStore.dispatch({
        type: ACTIONS.SET_RUNS,
        payload: { runs }
      });
    }, (err) => {
      console.log(err);
    });
};

const getNextRunTime = (params) => {
  MyPipelineApi
    .getNextRunTime(params)
    .subscribe(nextRunTime => {
      PipelineDetailStore.dispatch({
        type: ACTIONS.SET_NEXT_RUN_TIME,
        payload: { nextRunTime }
      });
    }, (err) => {
      console.log(err);
    });
};

const getStatistics = (params) => {
  MyPipelineApi
    .getStatistics(params)
    .subscribe(statistics => {
      PipelineDetailStore.dispatch({
        type: ACTIONS.SET_STATISTICS,
        payload: { statistics }
      });
    }, (err) => {
      console.log(err);
    });
};

const setMacros = (macrosMap) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_MACROS,
    payload: { macrosMap }
  });
};

const setUserRuntimeArguments = (argsMap) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_USER_RUNTIME_ARGUMENTS,
    payload: { argsMap }
  });
};

const fetchAndUpdateRuntimeArgs = () => {
  const params = {
    namespace: getCurrentNamespace(),
    appId: PipelineDetailStore.getState().name
  };

  let observable$ = MyPipelineApi.fetchMacros(params)
    .combineLatest([
      MyPreferenceApi.getAppPreferences(params),
      // This is required to resolve macros from preferences
      // Say DEFAULT_STREAM is a namespace level preference used as a macro
      // in one of the plugins in the pipeline.
      MyPreferenceApi.getAppPreferencesResolved(params)
    ]);

  observable$.subscribe((res) => {
    let macrosSpec = res[0];
    let macrosMap = {};
    let macros = [];
    macrosSpec.map(ms => {
      if (objectQuery(ms, 'spec', 'properties', 'macros', 'lookupProperties')) {
        macros = macros.concat(ms.spec.properties.macros.lookupProperties);
      }
    });
    macros.forEach(macro => {
      macrosMap[macro] = '';
    });

    let currentAppPrefs = res[1];
    let currentAppResolvedPrefs = res[2];
    let resolvedMacros = getMacrosResolvedByPrefs(currentAppResolvedPrefs, macrosMap);
    // When a pipeline is published there won't be any profile related information
    // at app level preference. However the pipeline, when run will be run with the 'default'
    // profile that is set at the namespace level. So we populate in UI the default
    // profile for a pipeline until the user choose something else. This is populated from
    // resolved app level preference which will provide preferences from namespace.
    const isProfileProperty = (property) => (
      [PROFILE_NAME_PREFERENCE_PROPERTY, PROFILE_PROPERTIES_PREFERENCE]
        .filter(profilePrefix => property.indexOf(profilePrefix) !== -1)
        .length
    );
    Object.keys(currentAppResolvedPrefs).forEach(resolvePref => {
      if (isProfileProperty(resolvePref) !== 0) {
        currentAppPrefs[resolvePref] = currentAppResolvedPrefs[resolvePref];
      }
    });

    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.SET_RESOLVED_MACROS,
      payload: { resolvedMacros }
    });
    const getPairs = (map) => (
      Object
        .entries(map)
        .filter(([key]) => key.length)
        .map(([key, value]) => ({key, value, uniqueId: uuidV4()}))
    );
    let runtimeArgsPairs = getPairs(currentAppPrefs);
    let resolveMacrosPairs = getPairs(resolvedMacros);

    PipelineConfigurationsStore.dispatch({
      type: PipelineConfigurationsActions.SET_RUNTIME_ARGS,
      payload: {
        runtimeArgs: {
          pairs: uniqBy(runtimeArgsPairs.concat(resolveMacrosPairs), (pair) => pair.key)
        }
      }
    });
  });
  return observable$;
};

const setMacrosAndUserRuntimeArguments = (macrosMap, argsMap) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_MACROS_AND_USER_RUNTIME_ARGUMENTS,
    payload: {
      macrosMap,
      argsMap
    }
  });
};

const setRuntimeArgsForDisplay = (args) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_RUNTIME_ARGUMENTS_FOR_DISPLAY,
    payload: { args }
  });
};

const setRunButtonLoading = (loading) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_RUN_BUTTON_LOADING,
    payload: { loading }
  });
};

const setRunError = (error) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_RUN_ERROR,
    payload: { error }
  });
};

const setScheduleButtonLoading = (loading) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_SCHEDULE_BUTTON_LOADING,
    payload: { loading }
  });
};

const setScheduleError = (error) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_SCHEDULE_ERROR,
    payload: { error }
  });
};

const setStopButtonLoading = (loading) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_STOP_BUTTON_LOADING,
    payload: { loading }
  });
};

const setStopError = (error) => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.SET_STOP_ERROR,
    payload: { error }
  });
};

const reset = () => {
  PipelineDetailStore.dispatch({
    type: ACTIONS.RESET
  });
};

export {
  init,
  setOptionalProperty,
  setSchedule,
  fetchScheduleStatus,
  setEngine,
  setBatchInterval,
  setMemoryMB,
  setVirtualCores,
  setDriverMemoryMB,
  setDriverVirtualCores,
  setClientMemoryMB,
  setClientVirtualCores,
  setBackpressure,
  setCustomConfig,
  setNumExecutors,
  setInstrumentation,
  setStageLogging,
  setCheckpointing,
  setNumRecordsPreview,
  setMaxConcurrentRuns,
  setCurrentRunId,
  getRuns,
  pollRuns,
  getNextRunTime,
  getStatistics,
  setMacros,
  setUserRuntimeArguments,
  setMacrosAndUserRuntimeArguments,
  setRuntimeArgsForDisplay,
  setRunButtonLoading,
  setRunError,
  setScheduleButtonLoading,
  setScheduleError,
  setStopButtonLoading,
  setStopError,
  reset,
  fetchAndUpdateRuntimeArgs
};
