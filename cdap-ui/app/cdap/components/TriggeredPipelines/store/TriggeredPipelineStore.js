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

import {combineReducers, createStore} from 'redux';
import TriggeredPipelineActions from 'components/TriggeredPipelines/store/TriggeredPipelineActions';

const defaultAction = {
  action : '',
  payload : {}
};

const defaultInitialState = {
  triggeredPipelines: [],
  expandedPipeline: null,
  pipelineInfoLoading: false,
  expandedPipelineInfo: null
};

const triggered = (state = defaultInitialState, action = defaultAction) => {
  switch (action.type) {
    case TriggeredPipelineActions.setTriggered:
      return {
        ...state,
        triggeredPipelines: action.payload.triggeredPipelines
      };

    case TriggeredPipelineActions.setToggle:
      return {
        ...state,
        expandedPipeline: action.payload.expandedPipeline,
        expandedPipelineInfo: null,
        pipelineInfoLoading: action.payload.expandedPipeline === null ? false : true
      };

    case TriggeredPipelineActions.setPipelineInfo:
      return {
        ...state,
        expandedPipelineInfo: action.payload.expandedPipelineInfo,
        pipelineInfoLoading: false
      };

    case TriggeredPipelineActions.reset:
      return defaultInitialState;
    default:
      return state;
  }
};

const TriggeredPipelineStore = createStore(
  combineReducers({
    triggered
  }),
  {
    triggered: defaultInitialState,
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default TriggeredPipelineStore;
