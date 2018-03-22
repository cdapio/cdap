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

import {defaultAction, composeEnhancers} from 'services/helpers';
import {createStore} from 'redux';
import {HYDRATOR_DEFAULT_VALUES} from 'services/global-constants';
import range from 'lodash/range';
import {convertMapToKeyValuePairsObj, keyValuePairsHaveMissingValues} from 'components/KeyValuePairs/KeyValueStoreActions';
import {getDefaultKeyValuePair} from 'components/KeyValuePairs/KeyValueStore';
import uuidV4 from 'uuid/v4';
import cloneDeep from 'lodash/cloneDeep';

const ACTIONS = {
  INITIALIZE_CONFIG: 'INITIALIZE_CONFIG',
  SET_RUNTIME_ARGS: 'SET_RUNTIME_ARGS',
  SET_SAVED_RUNTIME_ARGS: 'SET_SAVED_RUNTIME_ARGS',
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
  SET_NUM_RECORDS_PREVIEW: 'SET_NUM_RECORDS_PREVIEW',
  SET_PIPELINE_EDIT_STATUS: 'SET_PIPELINE_EDIT_STATUS',
  SET_MODELESS_OPEN_STATUS: 'SET_MODELESS_OPEN_STATUS',
  RESET: 'RESET'
};

const TAB_OPTIONS = {
  RUNTIME_ARGS: 'runtimeArgs',
  PREVIEW_CONFIG: 'previewConfig',
  PIPELINE_CONFIG: 'pipelineConfig',
  ENGINE_CONFIG: 'engineConfig',
  RESOURCES: 'resources',
  ALERTS: 'alerts',
  COMPUTECONFIG: 'computeConfig'
};

const BATCH_INTERVAL_RANGE = range(1, 61);
const BATCH_INTERVAL_UNITS = [
  {
    id: 's',
    value: 'Seconds'
  },
  {
    id: 'm',
    value: 'Minutes'
  },
];

const NUM_EXECUTORS_OPTIONS = range(1, 11);
const ENGINE_OPTIONS = {
  MAPREDUCE: 'mapreduce',
  SPARK: 'spark'
};

const DEFAULT_RUNTIME_ARGS = {
  'pairs': [getDefaultKeyValuePair()]
};

const DEFAULT_CONFIGURE_OPTIONS = {

  // savedRuntimeArgs represent the runtime args the user has Saved for the current session
  // runtimeArgs represent the current values in the modeless
  // If the user changes runtime args values in the modeless but doesn't click Save, then we'll
  // revert runtimeArgs to savedRuntimeArgs
  runtimeArgs: cloneDeep(DEFAULT_RUNTIME_ARGS),
  savedRuntimeArgs: cloneDeep(DEFAULT_RUNTIME_ARGS),
  resolvedMacros: {},
  customConfigKeyValuePairs: cloneDeep(DEFAULT_RUNTIME_ARGS),
  postRunActions: [],
  properties: {},
  engine: HYDRATOR_DEFAULT_VALUES.engine,
  resources: {...HYDRATOR_DEFAULT_VALUES.resources},
  driverResources: {...HYDRATOR_DEFAULT_VALUES.resources},
  clientResources: {...HYDRATOR_DEFAULT_VALUES.resources},
  processTimingEnabled: HYDRATOR_DEFAULT_VALUES.processTimingEnabled,
  stageLoggingEnabled: HYDRATOR_DEFAULT_VALUES.stageLoggingEnabled,
  disableCheckpoints: HYDRATOR_DEFAULT_VALUES.disableCheckpoints,
  stopGracefully: HYDRATOR_DEFAULT_VALUES.stopGracefully,
  backpressure: HYDRATOR_DEFAULT_VALUES.backpressure,
  numExecutors: HYDRATOR_DEFAULT_VALUES.numExecutors,
  numOfRecordsPreview: HYDRATOR_DEFAULT_VALUES.numOfRecordsPreview,
  previewTimeoutInMin: HYDRATOR_DEFAULT_VALUES.previewTimeoutInMin,
  batchInterval: HYDRATOR_DEFAULT_VALUES.batchInterval,
  postActions: [],
  schedule: HYDRATOR_DEFAULT_VALUES.cron,
  maxConcurrentRuns: 1,
  isMissingKeyValues: false,
  pipelineEdited: false,
  modelessOpen: false
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

const getCustomConfigForDisplay = (properties, engine) => {
  let currentCustomConfig = getCustomConfigFromProperties(properties);
  let customConfigForDisplay = {};
  for (let key in currentCustomConfig) {
    if (currentCustomConfig.hasOwnProperty(key)) {
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

const getEngineDisplayLabel = (engine, isBatch) => {
  return engine === ENGINE_OPTIONS.MAPREDUCE && isBatch ? 'MapReduce' : 'Apache Spark Streaming';
};

const checkForReset = (runtimeArgs, resolvedMacros) => {
  let runtimeArgsPairs = runtimeArgs.pairs;
  runtimeArgsPairs.forEach(runtimeArg => {
    if (!runtimeArg.notDeletable) {
      return;
    }
    if (runtimeArg.provided) {
      runtimeArg.showReset = false;
    } else {
      let runtimeArgKey = runtimeArg.key;
      if (resolvedMacros.hasOwnProperty(runtimeArgKey)) {
        if (resolvedMacros[runtimeArgKey] !== runtimeArg.value) {
          runtimeArg.showReset = true;
        } else {
          runtimeArg.showReset = false;
        }
      }
    }
  });
  return runtimeArgs;
};

const resetRuntimeArgToResolvedValue = (index, runtimeArgs, resolvedMacros) => {
  let runtimeArgKey = runtimeArgs.pairs[index].key;
  runtimeArgs.pairs[index].value = resolvedMacros[runtimeArgKey];
  return runtimeArgs;
};

const getRuntimeArgsForDisplay = (currentRuntimeArgs, macrosMap) => {
  let providedMacros = {};

  // holds provided macros in an object here even though we don't need the value,
  // because object hash is faster than Array.indexOf
  if (currentRuntimeArgs.pairs) {
    currentRuntimeArgs.pairs.forEach((currentPair) => {
      let key = currentPair.key;
      if (currentPair.notDeletable && currentPair.provided) {
        providedMacros[key] = currentPair.value;
      }
    });
    currentRuntimeArgs.pairs = currentRuntimeArgs.pairs.filter(keyValuePair => {
      return Object.keys(macrosMap).indexOf(keyValuePair.key) === -1;
    });
  }
  let macros = Object.keys(macrosMap).map(macroKey => {
    return {
      key: macroKey,
      value: macrosMap[macroKey],
      uniqueId: 'id-' + uuidV4(),
      notDeletable: true,
      provided: providedMacros.hasOwnProperty(macroKey)
    };
  });
  currentRuntimeArgs.pairs = macros.concat(currentRuntimeArgs.pairs);
  return currentRuntimeArgs;
};

const checkIfMissingKeyValues = (runtimeArguments, customConfig) => {
  return keyValuePairsHaveMissingValues(runtimeArguments) || keyValuePairsHaveMissingValues(customConfig);
};

const configure = (state = DEFAULT_CONFIGURE_OPTIONS, action = defaultAction) => {
  switch (action.type) {
    case ACTIONS.INITIALIZE_CONFIG:
      return {
        ...state,
        ...action.payload,
        customConfigKeyValuePairs: getCustomConfigForDisplay(action.payload.properties, action.payload.engine)
      };
    case ACTIONS.SET_RUNTIME_ARGS:
      return {
        ...state,
        runtimeArgs: checkForReset(action.payload.runtimeArgs, state.resolvedMacros),
        isMissingKeyValues: checkIfMissingKeyValues(action.payload.runtimeArgs, state.customConfigKeyValuePairs)
      };
    case ACTIONS.SET_SAVED_RUNTIME_ARGS:
      return {
        ...state,
        savedRuntimeArgs: action.payload.savedRuntimeArgs
      };
    case ACTIONS.SET_RESOLVED_MACROS: {
      let resolvedMacros = action.payload.resolvedMacros;
      let runtimeArgs = getRuntimeArgsForDisplay(cloneDeep(state.runtimeArgs), resolvedMacros);
      let savedRuntimeArgs = cloneDeep(runtimeArgs);
      let isMissingKeyValues = checkIfMissingKeyValues(runtimeArgs, state.customConfigKeyValuePairs);

      return {
        ...state,
        resolvedMacros,
        runtimeArgs,
        savedRuntimeArgs,
        isMissingKeyValues
      };
    }
    case ACTIONS.RESET_RUNTIME_ARG_TO_RESOLVED_VALUE:
      return {
        ...state,
        runtimeArgs: resetRuntimeArgToResolvedValue(action.payload.index, {...state.runtimeArgs}, state.resolvedMacros)
      };
    case ACTIONS.SET_ENGINE:
      return {
        ...state,
        engine: action.payload.engine
      };
    case ACTIONS.SET_BATCH_INTERVAL_RANGE:
      return {
        ...state,
        batchInterval: action.payload.batchIntervalRange + state.batchInterval.slice(-1)
      };
    case ACTIONS.SET_BATCH_INTERVAL_UNIT:
      return {
        ...state,
        batchInterval: state.batchInterval.slice(0, -1) + action.payload.batchIntervalUnit
      };
    case ACTIONS.SET_MEMORY_MB:
      return {
        ...state,
        resources: {
          ...state.resources,
          memoryMB: action.payload.memoryMB
        }
      };
    case ACTIONS.SET_MEMORY_VIRTUAL_CORES:
      return {
        ...state,
        resources: {
          ...state.resources,
          virtualCores: action.payload.virtualCores
        }
      };
    case ACTIONS.SET_DRIVER_MEMORY_MB:
      return {
        ...state,
        driverResources: {
          ...state.driverResources,
          memoryMB: action.payload.memoryMB
        }
      };
    case ACTIONS.SET_DRIVER_VIRTUAL_CORES:
      return {
        ...state,
        driverResources: {
          ...state.driverResources,
          virtualCores: action.payload.virtualCores
        }
      };
    case ACTIONS.SET_CLIENT_MEMORY_MB:
      return {
        ...state,
        clientResources: {
          ...state.clientResources,
          memoryMB: action.payload.memoryMB
        }
      };
    case ACTIONS.SET_CLIENT_VIRTUAL_CORES:
      return {
        ...state,
        clientResources: {
          ...state.clientResources,
          virtualCores: action.payload.virtualCores
        }
      };
    case ACTIONS.SET_BACKPRESSURE:
      return {
        ...state,
        properties: {
          ...state.properties,
          'system.spark.spark.streaming.backpressure.enabled': action.payload.backpressure
        }
      };
    case ACTIONS.SET_CUSTOM_CONFIG_KEY_VALUE_PAIRS:
      return {
        ...state,
        customConfigKeyValuePairs: action.payload.keyValues,
        isMissingKeyValues: checkIfMissingKeyValues(state.runtimeArgs, action.payload.keyValues)
      };
    case ACTIONS.SET_CUSTOM_CONFIG: {
      // Need to remove previous custom configs from config.properties before setting new ones
      let currentProperties = {...state.properties};
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
        if (action.payload.isBatch && state.engine === 'mapreduce') {
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
          ...newCustomConfigs
        }
      };
    }
    case ACTIONS.SET_NUM_EXECUTORS: {
      let numExecutorsKeyName = window.CDAP_CONFIG.isEnterprise ? 'system.spark.spark.executor.instances' : 'system.spark.spark.master';
      let numExecutorsValue = window.CDAP_CONFIG.isEnterprise ? action.payload.numExecutors : `local[${action.payload.numExecutors}]`;
      return {
        ...state,
        properties: {
          ...state.properties,
          [numExecutorsKeyName]: numExecutorsValue
        }
      };
    }
    case ACTIONS.SET_INSTRUMENTATION:
      return {
        ...state,
        processTimingEnabled: action.payload.instrumentation
      };
    case ACTIONS.SET_STAGE_LOGGING:
      return {
        ...state,
        stageLoggingEnabled: action.payload.stageLogging
      };
    case ACTIONS.SET_CHECKPOINTING:
      return {
        ...state,
        disableCheckpoints: action.payload.checkpointing
      };
    case ACTIONS.SET_NUM_RECORDS_PREVIEW:
      return {
        ...state,
        numOfRecordsPreview: action.payload.numRecordsPreview
      };
    case ACTIONS.SET_PIPELINE_EDIT_STATUS:
      return {
        ...state,
        pipelineEdited: action.payload.pipelineEdited
      };
    case ACTIONS.SET_MODELESS_OPEN_STATUS:
      return {
        ...state,
        modelessOpen: action.payload.open
      };
    case ACTIONS.RESET:
      return DEFAULT_CONFIGURE_OPTIONS;
    default:
      return state;
  }
};

const PipelineConfigurationsStore = createStore(
  configure,
  DEFAULT_CONFIGURE_OPTIONS,
  composeEnhancers('PipelineConfigurationsStore')()
);

export default PipelineConfigurationsStore;
export {
  ACTIONS,
  TAB_OPTIONS,
  BATCH_INTERVAL_RANGE,
  BATCH_INTERVAL_UNITS,
  NUM_EXECUTORS_OPTIONS,
  ENGINE_OPTIONS,
  DEFAULT_RUNTIME_ARGS,
  getCustomConfigForDisplay,
  getEngineDisplayLabel
};
