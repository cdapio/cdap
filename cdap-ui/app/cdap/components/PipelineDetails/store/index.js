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

const ACTIONS = {
  INITIALIZE_PIPELINE_DETAILS: 'INITIALIZE_PIPELINE_DETAILS',

  // Pipeline level Actions
  SET_OPTIONAL_PROPERTY: 'SET_OPTIONAL_PROPERTY',
  SET_SCHEDULE: 'SET_SCHEDULE',
  SET_CONFIG: 'SET_CONFIG',
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
    case ACTIONS.SET_CONFIG:
      return {
        ...state,
        config: action.payload.config
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
