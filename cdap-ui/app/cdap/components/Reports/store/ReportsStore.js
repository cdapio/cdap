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
import {defaultAction} from 'services/helpers';

const ReportsActions = {
  toggleCustomizerOption: 'REPORTS_TOGGLE_CUSTOMIZER_OPTION',
  setSelections: 'REPORTS_SET_SELECTIONS',
  setList: 'REPORTS_SET_LIST',
  setTimeRange: 'REPORTS_SET_TIME_RANGE',
  setRuns: 'REPORTS_SET_RUNS',
  setInfoStatus: 'REPORTS_SET_INFO_STATUS',
  clearSelection: 'REPORTS_CUSTOMIZER_CLEAR',
  setActiveId: 'REPORTS_SET_ACTIVE_ID',
  setStatus: 'REPORTS_SET_STATUS',
  setDetailsError: 'REPORTS_DETAILS_ERROR',
  detailsReset: 'REPORTS_DETAILS_RESET',
  setNamespaces: 'REPORTS_SET_NAMESPACES',
  reset: 'REPORTS_RESET'
};

const defaultCustomizerState = {
  pipelines: false,
  customApps: false,
  namespace: false,
  status: false,
  start: false,
  end: false,
  duration: false,
  user: false,
  startMethod: false,
  runtimeArgs: false,
  numLogWarnings: false,
  numLogErrors: false,
  numRecordsOut: false
};

const defaultStatusState = {
  statusSelections: []
};

const defaultTimeRangeState = {
  selection: null,
  start: null,
  end: null
};

const namespacesInitialState = {
  namespacesPick: []
};

const defaultListState = {
  total: 0,
  reports: [],
  offset: 0,
  activeId: null
};

const defaultDetailsState = {
  reportId: null,
  created: null,
  expiry: null,
  name: null,
  request: {},
  status: null,
  summary: {},
  runs: [],
  error: null,
  detailError: null
};

const customizer = (state = defaultCustomizerState, action = defaultAction) => {
  switch (action.type) {
    case ReportsActions.toggleCustomizerOption:
      return {
        ...state,
        [action.payload.type]: !state[action.payload.type]
      };
    case ReportsActions.setSelections:
      return {
        ...state,
        ...action.payload.selections
      };
    case ReportsActions.clearSelection:
    case ReportsActions.reset:
      return defaultCustomizerState;
    default:
      return state;
  }
};

const status = (state = defaultStatusState, action = defaultAction) => {
  switch (action.type) {
    case ReportsActions.setStatus:
    case ReportsActions.setSelections:
      return {
        ...state,
        statusSelections: action.payload.statusSelections
      };
    case ReportsActions.clearSelection:
    case ReportsActions.reset:
      return defaultStatusState;
    default:
      return state;
  }
};

const timeRange = (state = defaultTimeRangeState, action = defaultAction) => {
  switch (action.type) {
    case ReportsActions.setTimeRange:
      return {
        ...state,
        selection: action.payload.selection,
        start: action.payload.start,
        end: action.payload.end
      };
    case ReportsActions.setSelections:
      return {
        ...state,
        ...action.payload.timeRange
      };
    case ReportsActions.clearSelection:
    case ReportsActions.reset:
      return defaultTimeRangeState;
    default:
      return state;
  }
};

const namespaces = (state = namespacesInitialState, action = defaultAction) => {
  switch (action.type) {
    case ReportsActions.setSelections:
    case ReportsActions.setNamespaces:
      return {
        ...state,
        namespacesPick: action.payload.namespacesPick
      };
    case ReportsActions.clearSelection:
    case ReportsActions.reset:
      return namespacesInitialState;
    default:
      return state;
  }
};

const list = (state = defaultListState, action = defaultAction) => {
  switch (action.type) {
    case ReportsActions.setList:
      return {
        total: action.payload.list.total,
        reports: action.payload.list.reports,
        offset: action.payload.list.offset,
        activeId: action.payload.activeId
      };
    case ReportsActions.setActiveId:
      return {
        ...state,
        activeId: action.payload.activeId
      };
    case ReportsActions.reset:
      return defaultListState;
    default:
      return state;
  }
};

const details = (state = defaultDetailsState, action = defaultAction) => {
  switch (action.type) {
    case ReportsActions.setInfoStatus:
      return {
        ...state,
        ...action.payload.info,
        reportId: action.payload.reportId
      };
    case ReportsActions.setRuns:
      return {
        ...state,
        runs: action.payload.runs
      };
    case ReportsActions.setDetailsError:
      return {
        ...state,
        detailError: action.payload.error
      };
    case ReportsActions.detailsReset:
    case ReportsActions.reset:
      return defaultDetailsState;
    default:
      return state;
  }
};

const ReportsStore = createStore(
  combineReducers({
    customizer,
    status,
    list,
    details,
    timeRange,
    namespaces
  }),
  {
    customizer: defaultCustomizerState,
    status: defaultStatusState,
    list: defaultListState,
    details: defaultDetailsState,
    timeRange: defaultTimeRangeState,
    namespaces: namespacesInitialState
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default ReportsStore;
export {ReportsActions};
