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

const Actions = {
  setFields: 'FLL_SET_FIELDS',
  reset: 'FLL_RESET'
};

const defaultInitialState = {
  datasetId: '',
  fields: []
};

const lineage = (state = defaultInitialState, action = defaultAction) => {
  switch (action.type) {
    case Actions.setFields:
      return {
        ...state,
        datasetId: action.payload.datasetId,
        fields: action.payload.fields
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
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default Store;
export {Actions};
