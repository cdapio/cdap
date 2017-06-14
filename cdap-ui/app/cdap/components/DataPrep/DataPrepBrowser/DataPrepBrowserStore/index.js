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
  SET_DATABASE_ERROR: 'SET_DATABASE_ERROR',
  SET_KAFKA_PROPERTIES: 'SET_KAFKA_PROPERTIES',
  SET_KAFKA_LOADING: 'SET_KAFKA_LOADING',
  SET_KAFKA_ERROR: 'SET_KAFKA_ERROR'
};

export {Actions} ;

const defaultDatabaseValue = {
  info: {},
  tables: [],
  loading: false,
  error: null,
  connectionId: ''
};

const defaultKafkaValue = {
  info: {},
  topics: [],
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
        info: objectQuery(action, 'payload', 'info') || state.info,
        connectionId: objectQuery(action, 'payload', 'connectionId'),
        tables: objectQuery(action, 'payload', 'tables'),
        error: null
      });
    case Actions.SET_DATABASE_LOADING:
      return Object.assign({}, state, {
        loading: action.payload.loading,
        error: null
      });
    case Actions.SET_DATABASE_ERROR:
      return Object.assign({}, state, {
        error: action.payload.error,
        info: objectQuery(action, 'payload', 'info') || state.info,
        loading: false
      });
    default:
      return state;
  }
};

const kafka = (state = defaultKafkaValue, action = defaultAction) => {
  switch (action.type) {
    case Actions.SET_KAFKA_PROPERTIES:
      return Object.assign({}, state, {
        info: objectQuery(action, 'payload', 'info') || state.info,
        connectionId: objectQuery(action, 'payload', 'connectionId'),
        topics: objectQuery(action, 'payload', 'topics'),
        error: null
      });
    case Actions.SET_KAFKA_LOADING:
      return Object.assign({}, state, {
        loading: action.payload.loading,
        error: null
      });
    case Actions.SET_KAFKA_ERROR:
      return Object.assign({}, state, {
        error: action.payload.error,
        info: objectQuery(action, 'payload', 'info') || state.info,
        loading: false
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
    kafka,
    activeBrowser
  }),
  {
    database: defaultDatabaseValue,
    kafka: defaultKafkaValue,
    activeBrowser: defaultActiveBrowser
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default DataPrepBrowserStore;
