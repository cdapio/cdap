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
  setList: 'REPORTS_SET_LIST',
  setDetails: 'REPORTS_SET_DETAILS',
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
  runtimeArguments: false,
  numLogWarnings: false,
  numLogErrors: false,
  numRecordsOut: false
};

const defaultListState = {
  total: 0,
  reports: [],
  offset: 0
};

const defaultDetailsState = {
  summary: {},
  runs: []
};

const customizer = (state = defaultCustomizerState, action = defaultAction) => {
  switch (action.type) {
    case ReportsActions.toggleCustomizerOption:
      return {
        ...state,
        [action.payload.type]: !state[action.payload.type]
      };
    case ReportsActions.reset:
      return defaultCustomizerState;
    default:
      return state;
  }
};

const list = (state = defaultListState, action = defaultAction) => {
  switch (action.type) {
    case ReportsActions.setList:
      return {
        total: action.payload.total,
        reports: action.payload.reports,
        offset: action.payload.offset
      };
    case ReportsActions.reset:
      return defaultListState;
    default:
      return state;
  }
};

const details = (state = defaultDetailsState, action = defaultAction) => {
  switch (action.type) {
    case ReportsActions.setDetails:
      return {
        summary: action.payload.summary,
        runs: action.payload.runs
      };
    case ReportsActions.reset:
      return defaultDetailsState;
    default:
      return state;
  }
};

const ReportsStore = createStore(
  combineReducers({
    customizer,
    list,
    details
  }),
  {
    reports: defaultCustomizerState,
    list: defaultListState,
    details: defaultDetailsState
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default ReportsStore;
export {ReportsActions};
