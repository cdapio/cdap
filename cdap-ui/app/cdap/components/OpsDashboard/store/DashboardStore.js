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

const DashboardActions = {
  setDisplayBucket: 'DASHBOARD_SET_DISPLAY_BUCKET',
  toggleDisplayRuns: 'DASHBOARD_TOGGLE_DISPLAY_RUNS',
  reset: 'DASHBOARD_RESET'
};

const defaultAction = {
  action : '',
  payload : {}
};

const defaultInitialState = {
  data: [],
  pipelineCount: 0,
  customAppCount: 0,
  loading: false,
  displayRunsList: false,
  displayBucketInfo: null
};

const dashboard = (state = defaultInitialState, action = defaultAction) => {
  switch (action.type) {
    case DashboardActions.setDisplayBucket:
      return {
        ...state,
        displayBucketInfo: action.payload.displayBucketInfo,
        displayRunsList: true
      };
    case DashboardActions.toggleDisplayRuns:
      return {
        ...state,
        displayRunsList: !state.displayRunsList
      };
    case DashboardActions.reset:
      return defaultInitialState;
    default:
      return state;
  }
};

const DashboardStore = createStore(
  combineReducers({
    dashboard
  }),
  {
    dashboard: defaultInitialState
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default DashboardStore;
export {DashboardActions};
