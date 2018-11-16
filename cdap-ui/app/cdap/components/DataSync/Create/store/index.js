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

import { combineReducers, createStore } from 'redux';
import { defaultAction, composeEnhancers } from 'services/helpers';

const Actions = {
  setSource: 'DATASYNC_SET_SOURCE',
  setSourceConfig: 'DATASYNC_SET_SOURCE_CONFIG',
  setSink: 'DATASYNC_SET_SINK',
  setSinkConfig: 'DATASYNC_SET_SINK_CONFIG',
  setAdvancedConfig: 'DATASYNC_SET_ADVANCED_CONFIG',
  reset: 'DATASYNC_RESET',
};

const defaultInitialState = {
  activeStep: 0,
  source: null,
  sourceConfig: null,
  sink: null,
  sinkConfig: null,
  advancedConfig: null,
};

const datasync = (state = defaultInitialState, action = defaultAction) => {
  switch (action.type) {
    case Actions.setSource:
      return {
        ...state,
        source: action.payload.source,
        activeStep: 1,
      };
    case Actions.setSourceConfig:
      return {
        ...state,
        sourceConfig: action.payload.sourceConfig,
        activeStep: 2,
      };
    case Actions.setSink:
      return {
        ...state,
        sink: action.payload.sink,
        activeStep: 3,
      };
    case Actions.setSinkConfig:
      return {
        ...state,
        sinkConfig: action.payload.sinkConfig,
        activeStep: 5,
      };
    case Actions.setAdvancedConfig:
      return {
        ...state,
        advancedConfig: action.payload.advancedConfig,
        activeStep: 5,
      };
    case Actions.reset:
      return defaultInitialState;
    default:
      return state;
  }
};

const Store = createStore(
  combineReducers({
    datasync,
  }),
  {
    datasync: defaultInitialState,
  },
  composeEnhancers('DataSyncEnhancers')()
);

export default Store;
export { Actions };
