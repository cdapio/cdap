/*
 * Copyright © 2017 Cask Data, Inc.
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

import {combineReducers, createStore} from 'redux';
import PipelineTriggersActions from 'components/PipelineTriggers/store/PipelineTriggersActions';

const defaultAction = {
  action : '',
  payload : {}
};

const defaultInitialState = {
  pipelineList: [],
  selectedNamespace: '',
  enabledTriggers: [],
  pipelineName: '',
  expandedPipeline: null,
  expandedTrigger: null
};

const defaultInitialEnabledTriggersState = {
  loading: false,
  pipelineInfo: null
};

const triggers = (state = defaultInitialState, action = defaultAction) => {
  let stateCopy;

  switch (action.type) {
    case PipelineTriggersActions.setPipelineName:
      stateCopy = Object.assign({}, state, {
        pipelineName: action.payload.pipelineName
      });
      break;
    case PipelineTriggersActions.changeNamespace:
      stateCopy = Object.assign({}, state, {
        pipelineList: action.payload.pipelineList,
        selectedNamespace: action.payload.selectedNamespace,
        expandedPipeline: null
      });
      break;
    case PipelineTriggersActions.setTriggersAndPipelineList:
      stateCopy = Object.assign({}, state, {
        pipelineList: action.payload.pipelineList,
        enabledTriggers: action.payload.enabledTriggers,
        selectedNamespace: action.payload.selectedNamespace,
        expandedPipeline: null
      });
      break;
    case PipelineTriggersActions.setExpandedPipeline:
      stateCopy = Object.assign({}, state, {
        expandedPipeline: action.payload.expandedPipeline
      });
      break;
    case PipelineTriggersActions.setExpandedTrigger:
      stateCopy = Object.assign({}, state, {
        expandedTrigger: action.payload.expandedTrigger
      });
      break;
    case PipelineTriggersActions.reset:
      return defaultInitialState;
    default:
      return state;
  }

  return Object.assign({}, state, stateCopy);
};

const enabledTriggers = (state = defaultInitialEnabledTriggersState, action = defaultAction) => {
  let stateCopy;

  switch (action.type) {
    case PipelineTriggersActions.setExpandedTrigger:
      stateCopy = Object.assign({}, state, {
        loading: true
      });
      break;
    case PipelineTriggersActions.setEnabledTriggerPipelineInfo:
      stateCopy = Object.assign({}, state, {
        pipelineInfo: action.payload.pipelineInfo,
        loading: false
      });
      break;
    case PipelineTriggersActions.reset:
      return defaultInitialState;
    default:
      return state;
  }

  return Object.assign({}, state, stateCopy);
};

const PipelineTriggersStore = createStore(
  combineReducers({
    triggers,
    enabledTriggers
  }),
  {
    triggers: defaultInitialState,
    enabledTriggers: defaultInitialEnabledTriggersState
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default PipelineTriggersStore;
