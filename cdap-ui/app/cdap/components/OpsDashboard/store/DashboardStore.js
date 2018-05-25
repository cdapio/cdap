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
import {parseDashboardData} from 'components/OpsDashboard/RunsGraph/DataParser';

const DashboardActions = {
  setDisplayBucket: 'DASHBOARD_SET_DISPLAY_BUCKET',
  toggleDisplayRuns: 'DASHBOARD_TOGGLE_DISPLAY_RUNS',
  toggleLegend: 'DASHBOARD_TOGGLE_LEGEND',
  togglePipeline: 'DASHBOARD_TOGGLE_PIPELINE',
  toggleCustomApp: 'DAHBOARD_TOGGLE_CUSTOM_APP',
  enableLoading: 'DASHBOARD_ENABLE_LOADING',
  setData: 'DAHBOARD_SET_DATA',
  updateData: 'DASHBOARD_UPDATE_DATA',
  changeDisplayType: 'DASHBOARD_CHANGE_DISPLAY_TYPE',
  reset: 'DASHBOARD_RESET'
};

const defaultAction = {
  action : '',
  payload : {}
};

const defaultInitialState = {
  rawData: [],
  startTime: null,
  duration: null,
  data: [],
  pipelineCount: 0,
  customAppCount: 0,
  loading: false,
  displayRunsList: false,
  displayBucketInfo: null,
  pipeline: true,
  customApp: true,
  displayType: 'chart'
};

const legendsInitialState = {
  manual: true,
  schedule: true,
  success: true,
  failed: true,
  running: false,
  delay: true
};

const namespacesInitialState = {
  namespacesPick: []
};

const dashboard = (state = defaultInitialState, action = defaultAction) => {
  switch (action.type) {
    case DashboardActions.setData:
      return {
        ...state,
        rawData: action.payload.rawData,
        data: action.payload.data,
        pipelineCount: action.payload.pipelineCount,
        customAppCount: action.payload.customAppCount,
        startTime: action.payload.startTime,
        duration: action.payload.duration,
        displayRunsList: true,
        displayBucketInfo: action.payload.data[0],
        loading: false
      };
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
    case DashboardActions.togglePipeline:
      return {
        ...state,
        pipeline: !state.pipeline,
        data: parseDashboardData(state.rawData, state.startTime, state.duration, !state.pipeline, state.customApp).data
      };
    case DashboardActions.toggleCustomApp:
      return {
        ...state,
        customApp: !state.customApp,
        data: parseDashboardData(state.rawData, state.startTime, state.duration, state.pipeline, !state.customApp).data
      };
    case DashboardActions.enableLoading:
      return {
        ...state,
        loading: true
      };
    case DashboardActions.changeDisplayType:
      return {
        ...state,
        displayType: action.payload.displayType
      };
    case DashboardActions.reset:
      return defaultInitialState;
    default:
      return state;
  }
};

const legends = (state = legendsInitialState, action = defaultAction) => {
  switch (action.type) {
    case DashboardActions.toggleLegend:
      return {
        ...state,
        [action.payload.type]: !state[action.payload.type]
      };
    case DashboardActions.reset:
      return legendsInitialState;
    default:
      return state;
  }
};

const namespaces = (state = namespacesInitialState, action = defaultAction) => {
  switch (action.type) {
    case DashboardActions.setData:
      return {
        ...state,
        namespacesPick: action.payload.namespacesPick
      };
    case DashboardActions.reset:
      return namespacesInitialState;
    default:
      return state;
  }
};

const DashboardStore = createStore(
  combineReducers({
    dashboard,
    legends,
    namespaces
  }),
  {
    dashboard: defaultInitialState,
    legends: legendsInitialState,
    namespaces: namespacesInitialState
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default DashboardStore;
export {DashboardActions};
