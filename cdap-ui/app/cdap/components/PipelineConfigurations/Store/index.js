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

/*
  This store represents the state of the Pipeline Configure modeless.

  In Studio view, all the configs will have default values, while in Detail view
  this store is initialized using values from the pipeline json/PipelineDetailStore

  When the user makes a change inside the modeless and clicks Save, then we dispatch
  an action in PipelineConfigurations component to save the new configs to config-store.js
  (in Studio view) or to PipelineDetailStore (in Detail view)
*/

import { defaultAction, composeEnhancers } from 'services/helpers';
import { createStore } from 'redux';
import { HYDRATOR_DEFAULT_VALUES } from 'services/global-constants';
import range from 'lodash/range';
import {
  convertMapToKeyValuePairsObj,
  keyValuePairsHaveMissingValues,
} from 'components/KeyValuePairs/KeyValueStoreActions';
import { getDefaultKeyValuePair } from 'components/KeyValuePairs/KeyValueStore';
import uuidV4 from 'uuid/v4';
import cloneDeep from 'lodash/cloneDeep';
import {
  SPARK_EXECUTOR_INSTANCES,
  SPARK_BACKPRESSURE_ENABLED,
  ENGINE_OPTIONS,
} from 'components/PipelineConfigurations/PipelineConfigConstants';
import { GLOBALS } from 'services/global-constants';

const ACTIONS = {
  INITIALIZE_CONFIG: 'INITIALIZE_CONFIG',
  SET_RUNTIME_ARGS: 'SET_RUNTIME_ARGS',
  SET_RESOLVED_MACROS: 'SET_RESOLVED_MACROS',
  RESET_RUNTIME_ARG_TO_RESOLVED_VALUE: 'RESET_RUNTIME_ARG_TO_RESOLVED_VALUE',
  SET_ENGINE: 'SET_ENGINE',
  SET_BATCH_INTERVAL_RANGE: 'SET_BATCH_INTERVAL_RANGE',
  SET_BATCH_INTERVAL_UNIT: 'SET_BATCH_INTERVAL_UNIT',
  SET_MEMORY_MB: 'SET_MEMORY_MB',
  SET_MEMORY_VIRTUAL_CORES: 'SET_MEMORY_VIRTUAL_CORES',
  SET_DRIVER_MEMORY_MB: 'SET_DRIVER_MEMORY_MB',
  SET_DRIVER_VIRTUAL_CORES: 'SET_DRIVER_VIRTUAL_CORES',
  SET_CLIENT_MEMORY_MB: 'SET_CLIENT_MEMORY_MB',
  SET_CLIENT_VIRTUAL_CORES: 'SET_CLIENT_VIRTUAL_CORES',
  SET_BACKPRESSURE: 'SET_BACKPRESSURE',
  SET_CUSTOM_CONFIG: 'SET_CUSTOM_CONFIG',
  SET_CUSTOM_CONFIG_KEY_VALUE_PAIRS: 'SET_CUSTOM_CONFIG_KEY_VALUE_PAIRS',
  SET_NUM_EXECUTORS: 'SET_NUM_EXECUTORS',
  SET_INSTRUMENTATION: 'SET_INSTRUMENTATION',
  SET_STAGE_LOGGING: 'SET_STAGE_LOGGING',
  SET_CHECKPOINTING: 'SET_CHECKPOINTING',
  SET_CHECKPOINT_DIR: 'SET_CHECKPOINT_DIR',
  SET_NUM_RECORDS_PREVIEW: 'SET_NUM_RECORDS_PREVIEW',
  SET_MODELESS_OPEN_STATUS: 'SET_MODELESS_OPEN_STATUS',
  SET_PIPELINE_VISUAL_CONFIGURATION: 'SET_PIPELINE_VISUAL_CONFIGURATION',
  SET_SERVICE_ACCOUNT_PATH: 'SET_SERVICE_ACCOUNT_PATH',
  RESET: 'RESET',
};

const BATCH_INTERVAL_RANGE = range(1, 61);
const BATCH_INTERVAL_UNITS = [
  {
    id: 's',
    value: 'Seconds',
  },
  {
    id: 'm',
    value: 'Minutes',
  },
];

const NUM_EXECUTORS_OPTIONS = range(1, 11);

const DEFAULT_RUNTIME_ARGS = {
  pairs: [getDefaultKeyValuePair()],
};

const DEFAULT_CONFIGURE_OPTIONS = {
  runtimeArgs: cloneDeep(DEFAULT_RUNTIME_ARGS),
  resolvedMacros: {},
  customConfigKeyValuePairs: cloneDeep(DEFAULT_RUNTIME_ARGS),
  postRunActions: [],
  properties: {},
  engine: HYDRATOR_DEFAULT_VALUES.engine,
  resources: { ...HYDRATOR_DEFAULT_VALUES.resources },
  driverResources: { ...HYDRATOR_DEFAULT_VALUES.resources },
  clientResources: { ...HYDRATOR_DEFAULT_VALUES.resources },
  processTimingEnabled: HYDRATOR_DEFAULT_VALUES.processTimingEnabled,
  stageLoggingEnabled: HYDRATOR_DEFAULT_VALUES.stageLoggingEnabled,
  disableCheckpoints: HYDRATOR_DEFAULT_VALUES.disableCheckpoints,
  checkpointDir: window.CDAP_CONFIG.hydrator.defaultCheckpointDir,
  stopGracefully: HYDRATOR_DEFAULT_VALUES.stopGracefully,
  backpressure: HYDRATOR_DEFAULT_VALUES.backpressure,
  numExecutors: HYDRATOR_DEFAULT_VALUES.numExecutors,
  numOfRecordsPreview: HYDRATOR_DEFAULT_VALUES.numOfRecordsPreview,
  previewTimeoutInMin: HYDRATOR_DEFAULT_VALUES.previewTimeoutInMin,
  batchInterval: HYDRATOR_DEFAULT_VALUES.batchInterval,
  postActions: [],
  schedule: HYDRATOR_DEFAULT_VALUES.schedule,
  maxConcurrentRuns: 1,
  isMissingKeyValues: false,
  modelessOpen: false,

  pipelineVisualConfiguration: {
    pipelineType: GLOBALS.etlDataPipeline,
    isHistoricalRun: false,
    isPreview: false,
    isDetailView: false,
  },
};

const getCustomConfigFromProperties = (properties = {}, pipelineType) => {
  let backendProperties = [SPARK_BACKPRESSURE_ENABLED, SPARK_EXECUTOR_INSTANCES];
  if (GLOBALS.etlBatchPipelines.includes(pipelineType)) {
    backendProperties = [];
  }
  let customConfig = {};
  Object.keys(properties).forEach((key) => {
    if (backendProperties.indexOf(key) === -1) {
      customConfig[key] = properties[key];
    }
  });
  return customConfig;
};

const getCustomConfigForDisplay = (properties, engine, pipelineType) => {
  let currentCustomConfig = getCustomConfigFromProperties(properties, pipelineType);
  let customConfigForDisplay = {};
  for (let key in currentCustomConfig) {
    if (Object.prototype.hasOwnProperty.call(currentCustomConfig, key)) {
      let newKey = key;
      const mapReduceKey = 'system.mapreduce.';
      const sparkKey = 'system.spark.';
      if (engine === 'mapreduce' && key.startsWith(mapReduceKey)) {
        newKey = newKey.slice(mapReduceKey.length);
      } else if (key.startsWith(sparkKey)) {
        newKey = newKey.slice(sparkKey.length);
      }
      customConfigForDisplay[newKey] = currentCustomConfig[key];
    }
  }
  return convertMapToKeyValuePairsObj(customConfigForDisplay);
};

const getEngineDisplayLabel = (engine, pipelineType) => {
  return engine === ENGINE_OPTIONS.MAPREDUCE && GLOBALS.etlBatchPipelines.includes(pipelineType)
    ? 'MapReduce'
    : 'Apache Spark';
};

const checkForReset = (runtimeArgs, resolvedMacros) => {
  let runtimeArgsPairs = runtimeArgs.pairs;
  runtimeArgsPairs.forEach((runtimeArg) => {
    if (!runtimeArg.notDeletable) {
      return;
    }
    if (runtimeArg.provided) {
      runtimeArg.showReset = false;
    } else {
      let runtimeArgKey = runtimeArg.key;
      if (Object.prototype.hasOwnProperty.call(resolvedMacros, runtimeArgKey)) {
        if (resolvedMacros[runtimeArgKey] !== runtimeArg.value) {
          runtimeArg.showReset = true;
        } else {
          runtimeArg.showReset = false;
        }
      }
    }
  });
  return getRuntimeArgsForDisplay(runtimeArgs, resolvedMacros);
};

const resetRuntimeArgToResolvedValue = (index, runtimeArgs, resolvedMacros) => {
  let runtimeArgKey = runtimeArgs.pairs[index].key;
  runtimeArgs.pairs[index].value = resolvedMacros[runtimeArgKey];
  return runtimeArgs;
};

const getRuntimeArgsForDisplay = (currentRuntimeArgs, macrosMap) => {
  let providedMacros = {};
  let runtimeArgsMap = {};

  // holds provided macros in an object here even though we don't need the value,
  // because object hash is faster than Array.indexOf
  if (currentRuntimeArgs.pairs) {
    currentRuntimeArgs.pairs.forEach((currentPair) => {
      let key = currentPair.key;
      runtimeArgsMap[key] = currentPair.value || '';
      if (currentPair.notDeletable && currentPair.provided) {
        providedMacros[key] = currentPair.value;
      }
    });
    currentRuntimeArgs.pairs = currentRuntimeArgs.pairs.filter((keyValuePair) => {
      return Object.keys(macrosMap).indexOf(keyValuePair.key) === -1;
    });
  }
  let macros = Object.keys(macrosMap).map((macroKey) => {
    return {
      key: macroKey,
      value: runtimeArgsMap[macroKey] || '',
      showReset: macrosMap[macroKey].showReset,
      uniqueId: 'id-' + uuidV4(),
      notDeletable: true,
      provided: Object.prototype.hasOwnProperty.call(providedMacros, macroKey),
    };
  });
  currentRuntimeArgs.pairs = macros.concat(currentRuntimeArgs.pairs);
  return currentRuntimeArgs;
};

const checkIfMissingKeyValues = (runtimeArguments, customConfig) => {
  return (
    keyValuePairsHaveMissingValues(runtimeArguments) || keyValuePairsHaveMissingValues(customConfig)
  );
};

const configure = (state = DEFAULT_CONFIGURE_OPTIONS, action = defaultAction) => {
  switch (action.type) {
    case ACTIONS.INITIALIZE_CONFIG:
      return {
        ...state,
        ...action.payload,
        customConfigKeyValuePairs: getCustomConfigForDisplay(
          action.payload.properties,
          action.payload.engine,
          state.pipelineVisualConfiguration.pipelineType
        ),
      };
    case ACTIONS.SET_RUNTIME_ARGS:
      return {
        ...state,
        runtimeArgs: checkForReset(action.payload.runtimeArgs, state.resolvedMacros),
        isMissingKeyValues: checkIfMissingKeyValues(
          action.payload.runtimeArgs,
          state.customConfigKeyValuePairs
        ),
      };
    case ACTIONS.SET_RESOLVED_MACROS: {
      let resolvedMacros = action.payload.resolvedMacros;
      let runtimeArgs = getRuntimeArgsForDisplay(cloneDeep(state.runtimeArgs), resolvedMacros);
      let isMissingKeyValues = checkIfMissingKeyValues(
        runtimeArgs,
        state.customConfigKeyValuePairs
      );

      return {
        ...state,
        resolvedMacros,
        runtimeArgs,
        isMissingKeyValues,
      };
    }
    case ACTIONS.RESET_RUNTIME_ARG_TO_RESOLVED_VALUE:
      return {
        ...state,
        runtimeArgs: resetRuntimeArgToResolvedValue(
          action.payload.index,
          { ...state.runtimeArgs },
          state.resolvedMacros
        ),
      };
    case ACTIONS.SET_ENGINE:
      return {
        ...state,
        engine: action.payload.engine,
      };
    case ACTIONS.SET_BATCH_INTERVAL_RANGE:
      return {
        ...state,
        batchInterval: action.payload.batchIntervalRange + state.batchInterval.slice(-1),
      };
    case ACTIONS.SET_BATCH_INTERVAL_UNIT:
      return {
        ...state,
        batchInterval: state.batchInterval.slice(0, -1) + action.payload.batchIntervalUnit,
      };
    case ACTIONS.SET_MEMORY_MB:
      return {
        ...state,
        resources: {
          ...state.resources,
          memoryMB: action.payload.memoryMB,
        },
      };
    case ACTIONS.SET_MEMORY_VIRTUAL_CORES:
      return {
        ...state,
        resources: {
          ...state.resources,
          virtualCores: action.payload.virtualCores,
        },
      };
    case ACTIONS.SET_DRIVER_MEMORY_MB:
      return {
        ...state,
        driverResources: {
          ...state.driverResources,
          memoryMB: action.payload.memoryMB,
        },
      };
    case ACTIONS.SET_DRIVER_VIRTUAL_CORES:
      return {
        ...state,
        driverResources: {
          ...state.driverResources,
          virtualCores: action.payload.virtualCores,
        },
      };
    case ACTIONS.SET_CLIENT_MEMORY_MB:
      return {
        ...state,
        clientResources: {
          ...state.clientResources,
          memoryMB: action.payload.memoryMB,
        },
      };
    case ACTIONS.SET_CLIENT_VIRTUAL_CORES:
      return {
        ...state,
        clientResources: {
          ...state.clientResources,
          virtualCores: action.payload.virtualCores,
        },
      };
    case ACTIONS.SET_BACKPRESSURE:
      return {
        ...state,
        properties: {
          ...state.properties,
          'system.spark.spark.streaming.backpressure.enabled': action.payload.backpressure,
        },
      };
    case ACTIONS.SET_CUSTOM_CONFIG_KEY_VALUE_PAIRS:
      return {
        ...state,
        customConfigKeyValuePairs: action.payload.keyValues,
        isMissingKeyValues: checkIfMissingKeyValues(state.runtimeArgs, action.payload.keyValues),
      };
    case ACTIONS.SET_CUSTOM_CONFIG: {
      // Need to remove previous custom configs from config.properties before setting new ones
      let currentProperties = { ...state.properties };
      let currentCustomConfigs = getCustomConfigFromProperties(currentProperties);
      Object.keys(currentCustomConfigs).forEach((customConfigKey) => {
        if (Object.prototype.hasOwnProperty.call(currentProperties, customConfigKey)) {
          delete currentProperties[customConfigKey];
        }
      });

      // Need to add system.mapreduce or system.spark to beginning of the keys that the user added
      let newCustomConfigs = {};
      Object.keys(action.payload.customConfig).forEach((newCustomConfigKey) => {
        let newCustomConfigValue = action.payload.customConfig[newCustomConfigKey];
        if (
          GLOBALS.etlBatchPipelines.includes(action.payload.pipelineType) &&
          state.engine === ENGINE_OPTIONS.MAPREDUCE
        ) {
          newCustomConfigKey = 'system.mapreduce.' + newCustomConfigKey;
        } else {
          newCustomConfigKey = 'system.spark.' + newCustomConfigKey;
        }
        newCustomConfigs[newCustomConfigKey] = newCustomConfigValue;
      });

      return {
        ...state,
        properties: {
          ...currentProperties,
          ...newCustomConfigs,
        },
      };
    }
    case ACTIONS.SET_NUM_EXECUTORS: {
      let numExecutorsValue = action.payload.numExecutors;
      return {
        ...state,
        properties: {
          ...state.properties,
          [SPARK_EXECUTOR_INSTANCES]: numExecutorsValue,
        },
      };
    }
    case ACTIONS.SET_INSTRUMENTATION:
      return {
        ...state,
        processTimingEnabled: action.payload.instrumentation,
      };
    case ACTIONS.SET_STAGE_LOGGING:
      return {
        ...state,
        stageLoggingEnabled: action.payload.stageLogging,
      };
    case ACTIONS.SET_CHECKPOINTING:
      return {
        ...state,
        disableCheckpoints: action.payload.disableCheckpoints,
      };
    case ACTIONS.SET_CHECKPOINT_DIR:
      return {
        ...state,
        checkpointDir: action.payload.checkpointDir,
      };
    case ACTIONS.SET_NUM_RECORDS_PREVIEW:
      return {
        ...state,
        numOfRecordsPreview: action.payload.numRecordsPreview,
      };
    case ACTIONS.SET_MODELESS_OPEN_STATUS:
      return {
        ...state,
        modelessOpen: action.payload.open,
      };
    case ACTIONS.SET_SERVICE_ACCOUNT_PATH:
      return {
        ...state,
        serviceAccountPath: action.payload.serviceAccountPath,
      };
    case ACTIONS.RESET:
      return cloneDeep(DEFAULT_CONFIGURE_OPTIONS);
    case ACTIONS.SET_PIPELINE_VISUAL_CONFIGURATION:
      return {
        ...state,
        pipelineVisualConfiguration: {
          ...state.pipelineVisualConfiguration,
          ...action.payload.pipelineVisualConfiguration,
        },
      };
    default:
      return state;
  }
};

const PipelineConfigurationsStore = createStore(
  configure,
  cloneDeep(DEFAULT_CONFIGURE_OPTIONS),
  composeEnhancers('PipelineConfigurationsStore')()
);

export default PipelineConfigurationsStore;
export {
  ACTIONS,
  BATCH_INTERVAL_RANGE,
  BATCH_INTERVAL_UNITS,
  NUM_EXECUTORS_OPTIONS,
  ENGINE_OPTIONS,
  DEFAULT_RUNTIME_ARGS,
  getCustomConfigForDisplay,
  getEngineDisplayLabel,
};
