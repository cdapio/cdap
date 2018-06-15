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
import {defaultAction, composeEnhancers} from 'services/helpers';

const DashboardActions = {
  setDisplayBucket: 'DASHBOARD_SET_DISPLAY_BUCKET',
  togglePipeline: 'DASHBOARD_TOGGLE_PIPELINE',
  toggleCustomApp: 'DAHBOARD_TOGGLE_CUSTOM_APP',
  enableLoading: 'DASHBOARD_ENABLE_LOADING',
  setData: 'DAHBOARD_SET_DATA',
  updateData: 'DASHBOARD_UPDATE_DATA',
  changeDisplayType: 'DASHBOARD_CHANGE_DISPLAY_TYPE',
  changeViewByOption: 'DASHBOARD_CHANGE_VIEW_BY_OPTION',
  reset: 'DASHBOARD_RESET'
};

const ViewByOptions = {
  runStatus: 'RUN_STATUS',
  startMethod: 'START_METHOD'
};

const defaultInitialState = {
  rawData: [],
  startTime: null,
  duration: null,
  data: [],
  pipelineCount: 0,
  customAppCount: 0,
  loading: false,
  displayBucketInfo: null,
  pipeline: true,
  customApp: true,
  displayType: 'chart',
  viewByOption: ViewByOptions.runStatus
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
        displayBucketInfo: action.payload.data[action.payload.data.length - 1],
        loading: false
      };
    case DashboardActions.setDisplayBucket:
      return {
        ...state,
        displayBucketInfo: action.payload.displayBucketInfo,
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
    case DashboardActions.changeViewByOption:
      return {
        ...state,
        viewByOption: action.payload.viewByOption
      };
    case DashboardActions.reset:
      return defaultInitialState;
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
    namespaces
  }),
  {
    dashboard: defaultInitialState,
    namespaces: namespacesInitialState
  },
  composeEnhancers('DashboardStore')()
);

export default DashboardStore;
export {DashboardActions, ViewByOptions};
