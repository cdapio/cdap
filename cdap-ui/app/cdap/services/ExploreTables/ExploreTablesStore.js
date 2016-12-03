/*
 * Copyright © 2016 Cask Data, Inc.
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

import thunk from 'redux-thunk';
import {combineReducers, createStore, applyMiddleware, compose} from 'redux';
import ExploreTablesActions from 'services/ExploreTables/ExploreTablesActions';
import shortid from 'shortid';

const defaultAction = {
  type: '',
  payload: {},
  uniqueId: shortid.generate()
};

const defaultInitialState = {
  tables: []
};

const tables = (state = defaultInitialState.tables, action = defaultAction) => {
  switch (action.type) {
    case ExploreTablesActions.SET_TABLES:
      return [...action.payload.tables || []];
    default:
      return state;
  }
};

const composeEnhancers = window.__REDUX_DEVTOOLS_EXTENSION_COMPOSE__ || compose;
const ExploreTablesStore = createStore(
  combineReducers({
    tables
  }),
  defaultInitialState,
  composeEnhancers(
    applyMiddleware(thunk)
  )
);

export default ExploreTablesStore;
