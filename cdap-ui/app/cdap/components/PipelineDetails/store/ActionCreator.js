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
  MyPipelineApi
    .getRuns(params)
    .subscribe(runs => {
      PipelineDetailStore.dispatch({
        type: ACTIONS.SET_RUNS,
        payload: { runs }
      });
    }, (err) => {
      console.log(err);
    });
};

const pollRuns = (params) => {
  return MyPipelineApi
    .pollRuns(params)
    .subscribe(runs => {
      PipelineDetailStore.dispatch({
        type: ACTIONS.SET_RUNS,
        payload: { runs }
      });

      // When there are new runs, always set current run to most recent run
      if (runs.length) {
        PipelineDetailStore.dispatch({
          type: ACTIONS.SET_CURRENT_RUN_ID,
          payload: { runId: runs[0].runid }
        });
      }
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
  reset
};
