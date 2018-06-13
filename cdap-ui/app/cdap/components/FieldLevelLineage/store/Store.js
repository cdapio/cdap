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

const Actions = {
  setFields: 'FLL_SET_FIELDS',
  setBackwardLineage: 'FLL_SET_BACKWARD_LINEAGE',
  closeSummary: 'FLL_CLOSE_SUMMARY',
  setSearch: 'FLL_SET_SEARCH',
  reset: 'FLL_RESET'
};

const defaultInitialState = {
  datasetId: '',
  fields: [],
  backward: [],
  activeField: null,
  search: ''
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
    case Actions.reset:
      return defaultInitialState;
    default:
      return state;
  }
};

const Store = createStore(
  combineReducers({
    lineage
  }),
  {
    lineage: defaultInitialState
  },
  composeEnhancers('FieldLevelLineageStore')()
);

export default Store;
export {Actions};
