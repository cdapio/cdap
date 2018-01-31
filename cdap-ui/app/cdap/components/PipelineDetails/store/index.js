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

import {createStore} from 'redux';
import {defaultAction, composeEnhancers} from 'services/helpers';
import {GLOBALS} from 'services/global-constants';

const ACTIONS = {
  INITIALIZE_PIPELINE_DETAILS: 'INITIALIZE_PIPELINE_DETAILS',

  // Pipeline level Actions
  SET_OPTIONAL_PROPERTY: 'SET_OPTIONAL_PROPERTY',
  SET_SCHEDULE: 'SET_SCHEDULE',
  SET_ENGINE: 'SET_ENGINE',
  SET_BATCH_INTERVAL: 'SET_BATCH_INTERVAL',
  SET_MEMORY_MB: 'SET_MEMORY_MB',
  SET_MEMORY_VIRTUAL_CORES: 'SET_MEMORY_VIRTUAL_CORES',
  SET_DRIVER_MEMORY_MB: 'SET_DRIVER_MEMORY_MB',
  SET_DRIVER_VIRTUAL_CORES: 'SET_DRIVER_VIRTUAL_CORES',
  SET_CLIENT_MEMORY_MB: 'SET_CLIENT_MEMORY_MB',
  SET_CLIENT_VIRTUAL_CORES: 'SET_CLIENT_VIRTUAL_CORES',
  SET_BACKPRESSURE: 'SET_BACKPRESSURE',
  SET_CUSTOM_CONFIG: 'SET_CUSTOM_CONFIG',
  SET_NUM_EXECUTORS: 'SET_NUM_EXECUTORS',
  SET_INSTRUMENTATION: 'SET_INSTRUMENTATION',
  SET_STAGE_LOGGING: 'SET_STAGE_LOGGING',
  SET_CHECKPOINTING: 'SET_CHECKPOINTING',
  SET_NUM_RECORDS_PREVIEW: 'SET_NUM_RECORDS_PREVIEW',
  SET_MAX_CONCURRENT_RUNS: 'SET_MAX_CONCURRENT_RUNS',
  SET_SCHEDULE_STATUS: 'SET_SCHEDULE_STATUS',

  // Run level Actions
  SET_NEXT_RUN_TIME: 'SET_NEXT_RUN_TIME',
  SET_CURRENT_RUN: 'SET_CURRENT_RUN',
  SET_RUNS: 'SET_RUNS',
  SET_STATISTICS: 'SET_STATISTICS',
  SET_MACROS: 'SET_MACROS',
  SET_USER_RUNTIME_ARGUMENTS: 'SET_USER_RUNTIME_ARGUMENTS',
  SET_MACROS_AND_USER_RUNTIME_ARGUMENTS: 'SET_MACROS_AND_USER_RUNTIME_ARGUMENTS',
  SET_RUNTIME_ARGUMENTS_FOR_DISPLAY: 'SET_RUNTIME_ARGUMENTS_FOR_DISPLAY',
  RESET: 'RESET'
};

const getCustomConfigFromProperties = (properties) => {
  const backendProperties = ['system.spark.spark.streaming.backpressure.enabled', 'system.spark.spark.executor.instances', 'system.spark.spark.master'];
  let customConfig = {};
  Object.keys(properties).forEach(key => {
    if (backendProperties.indexOf(key) === -1) {
      customConfig[key] = properties[key];
    }
  });
  return customConfig;
};

const DEFAULT_PIPELINE_DETAILS = {
  // Pipeline level info
  name: '',
  description: '',
  artifact: {
    name: '',
    version: '',
    scope: ''
  },
  config: {},
  version: '',
  scheduleStatus: '',

  // Run level info
  runs: [],
  currentRun: {},
  nextRunTime: null,
  statistics: '',
  macrosMap: {},
  userRuntimeArgumentsMap: {},
  // `runtimeArgsForDisplay` combines `macrosMap` and `userRuntimeArgumentsMap` objects
  // to create an object that can be used as a prop to the KeyValuePairs component
  runtimeArgsForDisplay: {}
};

const pipelineDetails = (state = DEFAULT_PIPELINE_DETAILS, action = defaultAction) => {
  switch (action.type) {
    case ACTIONS.INITIALIZE_PIPELINE_DETAILS: {
      let pipeline = action.payload.pipeline;
      let newPipelineConfig = {...state.config};

      try {
        newPipelineConfig = JSON.parse(pipeline.configuration);
      } catch (e) {
        console.log('ERROR: cannot parse configuration');
        newPipelineConfig = {...state.config};
      }

      return {
        ...state,
        name: pipeline.name,
        description: pipeline.description,
        artifact: {
          name: pipeline.artifact.name,
          version: pipeline.artifact.version,
          scope: pipeline.artifact.scope
        },
        config: {...newPipelineConfig},
        version: pipeline.appVersion
      };
    }
    case ACTIONS.SET_OPTIONAL_PROPERTY:
      return {
        ...state,
        [action.payload.key]: action.payload.value
      };
    case ACTIONS.SET_SCHEDULE:
      return {
        ...state,
        config: {
          ...state.config,
          schedule: action.payload.schedule
        }
      };
    case ACTIONS.SET_SCHEDULE_STATUS:
      return {
        ...state,
        scheduleStatus: action.payload.scheduleStatus
      };
    case ACTIONS.SET_ENGINE:
      return {
        ...state,
        config: {
          ...state.config,
          engine: action.payload.engine
        }
      };
    case ACTIONS.SET_BATCH_INTERVAL:
      return {
        ...state,
        config: {
          ...state.config,
          batchInterval: action.payload.batchInterval
        }
      };
    case ACTIONS.SET_MEMORY_MB:
      return {
        ...state,
        config: {
          ...state.config,
          resources: {
            ...state.config.resources,
            memoryMB: action.payload.memoryMB
          }
        }
      };
    case ACTIONS.SET_MEMORY_VIRTUAL_CORES:
      return {
        ...state,
        config: {
          ...state.config,
          resources: {
            ...state.config.resources,
            virtualCores: action.payload.virtualCores
          }
        }
      };
    case ACTIONS.SET_DRIVER_MEMORY_MB:
      return {
        ...state,
        config: {
          ...state.config,
          driverResources: {
            ...state.config.driverResources,
            memoryMB: action.payload.memoryMB
          }
        }
      };
    case ACTIONS.SET_DRIVER_VIRTUAL_CORES:
      return {
        ...state,
        config: {
          ...state.config,
          driverResources: {
            ...state.config.driverResources,
            virtualCores: action.payload.virtualCores
          }
        }
      };
    case ACTIONS.SET_CLIENT_MEMORY_MB:
      return {
        ...state,
        config: {
          ...state.config,
          clientResources: {
            ...state.config.clientResources,
            memoryMB: action.payload.memoryMB
          }
        }
      };
    case ACTIONS.SET_CLIENT_VIRTUAL_CORES:
      return {
        ...state,
        config: {
          ...state.config,
          clientResources: {
            ...state.config.clientResources,
            virtualCores: action.payload.virtualCores
          }
        }
      };
    case ACTIONS.SET_BACKPRESSURE:
      return {
        ...state,
        config: {
          ...state.config,
          properties: {
            ...state.config.properties,
            'system.spark.spark.streaming.backpressure.enabled': action.payload.backpressure
          }
        }
      };
    case ACTIONS.SET_CUSTOM_CONFIG: {
      // Need to remove previous custom configs from config.properties before setting new ones
      let currentProperties = {...state.config.properties};
      let currentCustomConfigs = getCustomConfigFromProperties(currentProperties);
      Object.keys(currentCustomConfigs).forEach(customConfigKey => {
        if (currentProperties.hasOwnProperty(customConfigKey)) {
          delete currentProperties[customConfigKey];
        }
      });

      // Need to add system.mapreduce or system.spark to beginning of the keys that the user added
      let newCustomConfigs = {};
      Object.keys(action.payload.customConfig).forEach(newCustomConfigKey => {
        let newCustomConfigValue = action.payload.customConfig[newCustomConfigKey];
        if (GLOBALS.etlBatchPipelines.indexOf(state.artifact.name) !== -1 && state.config.engine === 'mapreduce') {
          newCustomConfigKey = 'system.mapreduce.' + newCustomConfigKey;
        } else {
          newCustomConfigKey = 'system.spark.' + newCustomConfigKey;
        }
        newCustomConfigs[newCustomConfigKey] = newCustomConfigValue;
      });

      return {
        ...state,
        config: {
          ...state.config,
          properties: {
            ...currentProperties,
            ...newCustomConfigs
          }
        }
      };
    }
    case ACTIONS.SET_NUM_EXECUTORS: {
      let numExecutorsKeyName = window.CDAP_CONFIG.isEnterprise ? 'system.spark.spark.executor.instances' : 'system.spark.spark.master';
      let numExecutorsValue = window.CDAP_CONFIG.isEnterprise ? action.payload.numExecutors : `local[${action.payload.numExecutors}]`;
      return {
        ...state,
        config: {
          ...state.config,
          properties: {
            ...state.config.properties,
            [numExecutorsKeyName]: numExecutorsValue
          }
        }
      };
    }
    case ACTIONS.SET_INSTRUMENTATION:
      return {
        ...state,
        config: {
          ...state.config,
          processTimingEnabled: action.payload.instrumentation
        }
      };
    case ACTIONS.SET_STAGE_LOGGING:
      return {
        ...state,
        config: {
          ...state.config,
          stageLoggingEnabled: action.payload.stageLogging
        }
      };
    case ACTIONS.SET_CHECKPOINTING:
      return {
        ...state,
        config: {
          ...state.config,
          disableCheckpoints: action.payload.checkpointing
        }
      };
    case ACTIONS.SET_NUM_RECORDS_PREVIEW:
      return {
        ...state,
        config: {
          ...state.config,
          numOfRecordsPreview: action.payload.numRecordsPreview
        }
      };
    case ACTIONS.SET_MAX_CONCURRENT_RUNS:
      return {
        ...state,
        config: {
          ...state.config,
          maxConcurrentRuns: action.payload.maxConcurrentRuns
        }
      };
    case ACTIONS.SET_NEXT_RUN_TIME:
      return {
        ...state,
        nextRunTime: action.payload.nextRunTime
      };
    case ACTIONS.SET_CURRENT_RUN: {
      let currentRunId = action.payload.runId;
      let currentRun = state.runs.find(run => run.runid === currentRunId);
      if (!currentRun) {
        currentRun = state.runs[0];
      }
      return {
        ...state,
        currentRun,
      };
    }
    case ACTIONS.SET_RUNS:
      return {
        ...state,
        runs: action.payload.runs,
        currentRun: Object.keys(state.currentRun).length ? state.currentRun : action.payload.runs[0]
      };
    case ACTIONS.SET_STATISTICS:
      return {
        ...state,
        statistics: action.payload.statistics
      };
    case ACTIONS.SET_MACROS:
      return {
        ...state,
        macrosMap: action.payload.macrosMap
      };
    case ACTIONS.SET_USER_RUNTIME_ARGUMENTS:
      return {
        ...state,
        userRuntimeArgumentsMap: action.payload.argsMap
      };
    case ACTIONS.SET_MACROS_AND_USER_RUNTIME_ARGUMENTS:
      return {
        ...state,
        macrosMap: action.payload.macrosMap,
        userRuntimeArgumentsMap: action.payload.argsMap
      };
    case ACTIONS.SET_RUNTIME_ARGUMENTS_FOR_DISPLAY:
      return {
        ...state,
        runtimeArgsForDisplay: action.payload.args
      };
    case ACTIONS.RESET:
      return DEFAULT_PIPELINE_DETAILS;
    default:
      return state;
  }
};

const PipelineDetailStore = createStore(
  pipelineDetails,
  DEFAULT_PIPELINE_DETAILS,
  composeEnhancers('PipelineDetailStore')()
);

export default PipelineDetailStore;
export {ACTIONS};
