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
import {defaultAction, composeEnhancers} from 'services/helpers';

export const TIME_OPTIONS = [
  'last7d',
  'last14d',
  'lastMonth',
  'last6M',
  'lastYear'
];

const Actions = {
  setFields: 'FLL_SET_FIELDS',
  setBackwardLineage: 'FLL_SET_BACKWARD_LINEAGE',
  closeSummary: 'FLL_CLOSE_SUMMARY',
  setSearch: 'FLL_SET_SEARCH',
  setBackwardOperations: 'FLL_SET_BACKWARD_OPERATIONS',
  closeOperations: 'FLL_CLOSE_OPERATIONS',
  operationsLoading: 'FLL_OPERATIONS_LOADING',
  nextOperation: 'FLL_NEXT_OPERATION',
  prevOperation: 'FLL_PREV_OPERATION',
  setTimeSelection: 'FLL_SET_TIME_SELECTION',
  reset: 'FLL_RESET'
};

const defaultInitialState = {
  datasetId: '',
  fields: [],
  backward: [],
  activeField: null,
  search: '',
  timeSelection: TIME_OPTIONS[0]
};

const operationsInitialState = {
  backwardOperations: [],
  showOperations: false,
  activeIndex: 0,
  loading: false
};

const lineage = (state = defaultInitialState, action = defaultAction) => {
  switch (action.type) {
    case Actions.setFields:
      return {
        ...state,
        datasetId: action.payload.datasetId,
        fields: action.payload.fields
      };
    case Actions.setBackwardLineage:
      return {
        ...state,
        backward: action.payload.backward,
        activeField: action.payload.activeField
      };
    case Actions.closeSummary:
      return {
        ...state,
        activeField: null
      };
    case Actions.setSearch:
      return {
        ...state,
        search: action.payload.search
      };
    case Actions.setTimeSelection:
      return {
        ...state,
        timeSelection: action.payload.timeSelection
      };
    case Actions.reset:
      return defaultInitialState;
    default:
      return state;
  }
};

const operations = (state = operationsInitialState, action = defaultAction) => {
  switch (action.type) {
    case Actions.operationsLoading:
      return {
        ...state,
        loading: true,
        showOperations: true
      };
    case Actions.setBackwardOperations:
      return {
        ...state,
        backwardOperations: action.payload.backwardOperations,
        activeIndex: 0,
        showOperations: true,
        loading: false
      };
    case Actions.setBackwardLineage:
    case Actions.closeOperations:
      return {
        ...state,
        backwardOperations: [],
        showOperations: false
      };
    case Actions.nextOperation:
      return {
        ...state,
        activeIndex: state.activeIndex + 1
      };
    case Actions.prevOperation:
      return {
        ...state,
        activeIndex: state.activeIndex - 1
      };
    case Actions.reset:
      return defaultInitialState;
    default:
      return state;
  }
};

const Store = createStore(
  combineReducers({
    lineage,
    operations
  }),
  {
    lineage: defaultInitialState,
    operations: operationsInitialState
  },
  composeEnhancers('FieldLevelLineageStore')()
);

export default Store;
export {Actions};
