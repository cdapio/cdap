/*
 * Copyright © 2017 Cask Data, Inc.
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

import {createStore, combineReducers} from 'redux';
import {defaultAction} from 'services/helpers';
import {objectQuery} from 'services/helpers';
const Actions = {
  SET_DATABASE_PROPERTIES: 'SET_DATABASE_PROPERTIES',
  SET_DATABASE_LOADING: 'SET_DATABASE_LOADING',
  SET_ACTIVEBROWSER: 'SET_ACTIVE_BROWSER',
  SET_DATABASE_ERROR: 'SET_DATABASE_ERROR'
};

export {Actions} ;

const defaultDatabaseValue = {
  properties: {},
  loading: false,
  error: null,
  connectionId: ''
};

const defaultActiveBrowser = {
  name: 'database'
};

const database = (state = defaultDatabaseValue, action = defaultAction) => {
  switch (action.type) {
    case Actions.SET_DATABASE_PROPERTIES:
      return Object.assign({}, state, {
        properties: objectQuery(action, 'payload', 'properties') || state.properties,
        connectionId: objectQuery(action, 'payload', 'connectionId'),
        error: null
      });
    case Actions.SET_DATABASE_LOADING:
      return Object.assign({}, state, {
        loading: action.payload.loading,
        error: null
      });
    case Actions.SET_DATABASE_ERROR:
      return Object.assign({}, state, {
        error: action.payload.error
      });
    default:
      return state;
  }
};

const activeBrowser = (state = defaultActiveBrowser, action = defaultAction) => {
  switch (action.type) {
    case Actions.SET_ACTIVEBROWSER:
      return Object.assign({}, state, {
        name: action.payload.name
      });
    default:
      return state;
  }
};

const DataPrepBrowserStore = createStore(
  combineReducers({
    database,
    activeBrowser
  }),
  {
    database: defaultDatabaseValue,
    activeBrowser: defaultActiveBrowser
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default DataPrepBrowserStore;
