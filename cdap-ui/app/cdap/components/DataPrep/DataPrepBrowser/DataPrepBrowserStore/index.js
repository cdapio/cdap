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
  SET_KAFKA_ERROR: 'SET_KAFKA_ERROR',
  SET_S3_LOADING: 'SET_S3_LOADING',
  SET_S3_ACTIVE_BUCKET_DETAILS: 'SET_S3_ACTIVE_BUCKET_DETAILS',
  SET_S3_CONNECTION_DETAILS: 'SET_S3_CONNECTION_DETAILS',
  SET_S3_CONNECTION_ID: 'SET_S3_CONNECTION_ID',
  SET_S3_PREFIX: 'SET_S3_PREFIX',
  SET_S3_DATAVIEW: 'SET_S3_DATAVIEW',
  SET_S3_SEARCH: 'SET_S3_SEARCH'
};

export {Actions};

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

const defaultS3Value = {
  info: {},
  loading: false,
  error: null,
  activeBucketDetails: [],
  prefix: '',
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
        error: null,
        loading: false
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
        error: null,
        loading: false
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

const s3 = (state = defaultS3Value, action = defaultAction) => {
  switch (action.type) {
    case Actions.SET_S3_CONNECTION_ID:
      // This means the user is starting afresh. Reset everything to default and set the connectionID
      return {
        ...defaultS3Value,
        connectionId: action.payload.connectionId
      };
    case Actions.SET_S3_CONNECTION_DETAILS:
      return {
        ...state,
        info: action.payload.info,
        error: null
      };
    case Actions.SET_S3_LOADING:
      return {
        ...state,
        loading: true
      };
    case Actions.SET_S3_ACTIVE_BUCKET_DETAILS:
      return {
        ...state,
        activeBucketDetails: action.payload.activeBucketDetails,
        loading: false
      };
    case Actions.SET_S3_PREFIX:
      return {
        ...state,
        prefix: action.payload.prefix
      };
    case Actions.SET_S3_SEARCH:
      return {
        ...state,
        search: action.payload.search
      };
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
    activeBrowser,
    s3
  }),
  {
    database: defaultDatabaseValue,
    kafka: defaultKafkaValue,
    activeBrowser: defaultActiveBrowser
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default DataPrepBrowserStore;
