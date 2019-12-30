/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import { createStore, combineReducers } from 'redux';
import { defaultAction, composeEnhancers, objectQuery } from 'services/helpers';

const Actions = {
  // File
  SET_FILE_SYSTEM_CONTENTS: 'SET_FILE_SYSTEM_CONTENTS',
  SET_FILE_SYSTEM_PATH: 'SET_FILE_SYSTEM_PATH',
  SET_FILE_SYSTEM_LOADING: 'SET_FILE_SYSTEM_LOADING',
  SET_FILE_SYSTEM_SEARCH: 'SET_FILE_SYSTEM_SEARCH',

  // Database
  SET_DATABASE_PROPERTIES: 'SET_DATABASE_PROPERTIES',
  SET_DATABASE_CONNECTION_ID: 'SET_DATABASE_CONNECTION_ID',
  SET_DATABASE_LOADING: 'SET_DATABASE_LOADING',
  SET_ACTIVEBROWSER: 'SET_ACTIVE_BROWSER',

  // Kafka
  SET_KAFKA_PROPERTIES: 'SET_KAFKA_PROPERTIES',
  SET_KAFKA_CONNECTION_ID: 'SET_KAFKA_CONNECTION_ID',
  SET_KAFKA_LOADING: 'SET_KAFKA_LOADING',

  // S3
  SET_S3_LOADING: 'SET_S3_LOADING',
  SET_S3_ACTIVE_BUCKET_DETAILS: 'SET_S3_ACTIVE_BUCKET_DETAILS',
  SET_S3_CONNECTION_DETAILS: 'SET_S3_CONNECTION_DETAILS',
  SET_S3_CONNECTION_ID: 'SET_S3_CONNECTION_ID',
  SET_S3_PREFIX: 'SET_S3_PREFIX',
  SET_S3_DATAVIEW: 'SET_S3_DATAVIEW',
  SET_S3_SEARCH: 'SET_S3_SEARCH',

  // GCS
  SET_GCS_LOADING: 'SET_GCS_LOADING',
  SET_GCS_ACTIVE_BUCKET_DETAILS: 'SET_GCS_ACTIVE_BUCKET_DETAILS',
  SET_GCS_CONNECTION_DETAILS: 'SET_GCS_CONNECTION_DETAILS',
  SET_GCS_CONNECTION_ID: 'SET_GCS_CONNECTION_ID',
  SET_GCS_PREFIX: 'SET_GCS_PREFIX',
  SET_GCS_DATAVIEW: 'SET_GCS_DATAVIEW',
  SET_GCS_SEARCH: 'SET_GCS_SEARCH',

  // Big Query
  SET_BIGQUERY_CONNECTION_ID: 'SET_BIGQUERY_CONNECTION_ID',
  SET_BIGQUERY_CONNECTION_DETAILS: 'SET_BIGQUERY_CONNECTION_DETAILS',
  SET_BIGQUERY_LOADING: 'SET_BIGQUERY_LOADING',
  SET_BIGQUERY_DATASET_LIST: 'SET_BIGQUERY_DATASET_LIST',
  SET_BIGQUERY_TABLE_LIST: 'SET_BIGQUERY_TABLE_LIST',

  // Spanner
  SET_SPANNER_CONNECTION_ID: 'SET_SPANNER_CONNECTION_ID',
  SET_SPANNER_CONNECTION_DETAILS: 'SET_SPANNER_CONNECTION_DETAILS',
  SET_SPANNER_LOADING: 'SET_SPANNER_LOADING',
  SET_SPANNER_INSTANCE_LIST: 'SET_SPANNER_INSTANCE_LIST',
  SET_SPANNER_DATABASE_LIST: 'SET_SPANNER_DATABASE_LIST',
  SET_SPANNER_TABLE_LIST: 'SET_SPANNER_TABLE_LIST',

  // adls
  SET_ADLS_PROPERTIES: 'SET_ADLS_PROPERTIES',
  SET_ADLS_CONNECTION_ID: 'SET_ADLS_CONNECTION_ID',
  SET_ADLS_LOADING: 'SET_ADLS_LOADING',
  SET_ADLS_PREFIX: 'SET_ADLS_PREFIX',
  SET_ADLS_CONNECTION_DETAILS: 'SET_ADLS_CONNECTION_DETAILS',
  SET_ADLS_FILE_SYSTEM_CONTENTS: 'SET_ADLS_FILE_SYSTEM_CONTENTS',
  SET_ADLS_FILE_SYSTEM_PATH: 'SET_ADLS_FILE_SYSTEM_PATH',
  SET_ADLS_FILE_SYSTEM_LOADING: 'SET_ADLS_FILE_SYSTEM_LOADING',
  SET_ADLS_FILE_SYSTEM_SEARCH: 'SET_ADLS_FILE_SYSTEM_SEARCH',

  SET_ERROR: 'SET_ERROR',
  RESET: 'RESET',
};

export { Actions };

const defaultFileSystemValue = {
  contents: [],
  loading: false,
  path: '',
  search: '',
};

const defaultDatabaseValue = {
  info: {},
  tables: [],
  loading: false,
  connectionId: '',
};

const defaultKafkaValue = {
  info: {},
  topics: [],
  loading: false,
  connectionId: '',
};

const defaultS3Value = {
  info: {},
  loading: false,
  activeBucketDetails: [],
  truncated: false,
  prefix: '',
  connectionId: '',
  search: '',
};

const defaultGCSValue = {
  info: {},
  loading: false,
  activeBucketDetails: [],
  truncated: false,
  prefix: '',
  connectionId: '',
  search: '',
};

const defaultBigQueryValue = {
  info: {},
  loading: false,
  connectionId: '',
  datasetId: null,
  datasetList: [],
  tableList: [],
};

const defaultSpannerValue = {
  info: {},
  loading: false,
  connectionId: '',
  instanceId: null,
  dabaseId: null,
  instanceList: [],
  databaseList: [],
  tableList: [],
};

const defaultADLSValue = {
  info: {},
  connectionId: '',
  contents: [],
  path: '',
  search: '',
  loading: false,
};

const defaultActiveBrowser = {
  name: '',
};

const defaultError = null;

// TODO: Right now each connection type maintains its own `loading` state. However,
// we can just have one loading state for all connection types, similar to how
// we implement `error` state here.
// JIRA: CDAP-14173

const file = (state = defaultFileSystemValue, action = defaultAction) => {
  switch (action.type) {
    case Actions.SET_FILE_SYSTEM_CONTENTS:
      return {
        ...state,
        loading: false,
        contents: action.payload.contents,
        search: '',
      };
    case Actions.SET_FILE_SYSTEM_PATH:
      return {
        ...state,
        path: action.payload.path,
        search: '',
      };
    case Actions.SET_FILE_SYSTEM_LOADING:
      return {
        ...state,
        loading: action.payload.loading,
      };
    case Actions.SET_ERROR:
      return {
        ...state,
        loading: false,
        search: '',
      };
    case Actions.RESET:
      return defaultFileSystemValue;
    default:
      return state;
  }
};

const database = (state = defaultDatabaseValue, action = defaultAction) => {
  switch (action.type) {
    case Actions.SET_DATABASE_CONNECTION_ID: {
      if (action.payload.connectionId === state.connectionId) {
        return state;
      }
      // This means the user is starting afresh. Reset everything to default and set the connectionID
      return {
        ...defaultDatabaseValue,
        connectionId: action.payload.connectionId,
      };
    }
    case Actions.SET_DATABASE_PROPERTIES:
      return Object.assign({}, state, {
        info: objectQuery(action, 'payload', 'info') || state.info,
        connectionId: objectQuery(action, 'payload', 'connectionId') || state.connectionId,
        tables: objectQuery(action, 'payload', 'tables'),
        error: null,
        loading: false,
      });
    case Actions.SET_DATABASE_LOADING:
      return Object.assign({}, state, {
        loading: action.payload.loading,
        error: null,
      });
    case Actions.SET_ERROR:
      return {
        ...state,
        loading: false,
      };
    case Actions.SET_ACTIVEBROWSER:
      return defaultDatabaseValue;
    case Actions.RESET:
      return defaultDatabaseValue;
    default:
      return state;
  }
};

const kafka = (state = defaultKafkaValue, action = defaultAction) => {
  switch (action.type) {
    case Actions.SET_KAFKA_CONNECTION_ID: {
      if (action.payload.connectionId === state.connectionId) {
        return state;
      }
      // This means the user is starting afresh. Reset everything to default and set the connectionID
      return {
        ...defaultKafkaValue,
        connectionId: action.payload.connectionId,
      };
    }
    case Actions.SET_KAFKA_PROPERTIES:
      return Object.assign({}, state, {
        info: objectQuery(action, 'payload', 'info') || state.info,
        connectionId: objectQuery(action, 'payload', 'connectionId') || state.connectionId,
        topics: objectQuery(action, 'payload', 'topics'),
        error: null,
        loading: false,
      });
    case Actions.SET_KAFKA_LOADING:
      return Object.assign({}, state, {
        loading: action.payload.loading,
        error: null,
      });
    case Actions.SET_ERROR:
      return {
        ...state,
        loading: false,
      };
    case Actions.SET_ACTIVEBROWSER:
      return defaultKafkaValue;
    case Actions.RESET:
      return defaultKafkaValue;
    default:
      return state;
  }
};

const s3 = (state = defaultS3Value, action = defaultAction) => {
  switch (action.type) {
    case Actions.SET_S3_CONNECTION_ID: {
      if (action.payload.connectionId === state.connectionId) {
        return state;
      }
      // This means the user is starting afresh. Reset everything to default and set the connectionID
      return {
        ...defaultS3Value,
        connectionId: action.payload.connectionId,
      };
    }
    case Actions.SET_S3_CONNECTION_DETAILS:
      return {
        ...state,
        info: action.payload.info,
        connectionId: objectQuery(action, 'payload', 'connectionId') || state.connectionId,
        error: null,
      };
    case Actions.SET_S3_LOADING:
      return {
        ...state,
        loading: true,
      };
    case Actions.SET_S3_ACTIVE_BUCKET_DETAILS:
      return {
        ...state,
        activeBucketDetails: action.payload.activeBucketDetails,
        truncated: action.payload.truncated,
        loading: false,
      };
    case Actions.SET_S3_PREFIX:
      return {
        ...state,
        prefix: action.payload.prefix,
      };
    case Actions.SET_S3_SEARCH:
      return {
        ...state,
        search: action.payload.search,
      };
    case Actions.SET_ERROR:
      return {
        ...state,
        loading: false,
      };
    case Actions.SET_ACTIVEBROWSER:
      return defaultS3Value;
    case Actions.RESET:
      return defaultS3Value;
    default:
      return state;
  }
};

const gcs = (state = defaultGCSValue, action = defaultAction) => {
  switch (action.type) {
    case Actions.SET_GCS_CONNECTION_ID: {
      if (action.payload.connectionId === state.connectionId) {
        return state;
      }
      // This means the user is starting afresh. Reset everything to default and set the connectionID
      return {
        ...defaultGCSValue,
        connectionId: action.payload.connectionId,
      };
    }
    case Actions.SET_GCS_CONNECTION_DETAILS:
      return {
        ...state,
        info: action.payload.info,
        connectionId: objectQuery(action, 'payload', 'connectionId') || state.connectionId,
        error: null,
      };
    case Actions.SET_GCS_LOADING:
      return {
        ...state,
        loading: true,
      };
    case Actions.SET_GCS_ACTIVE_BUCKET_DETAILS:
      return {
        ...state,
        activeBucketDetails: action.payload.activeBucketDetails,
        truncated: action.payload.truncated,
        loading: false,
      };
    case Actions.SET_GCS_PREFIX:
      return {
        ...state,
        prefix: action.payload.prefix,
      };
    case Actions.SET_GCS_SEARCH:
      return {
        ...state,
        search: action.payload.search,
      };
    case Actions.SET_ERROR:
      return {
        ...state,
        loading: false,
      };
    case Actions.SET_ACTIVEBROWSER:
      return defaultGCSValue;
    case Actions.RESET:
      return defaultGCSValue;
    default:
      return state;
  }
};

const bigquery = (state = defaultBigQueryValue, action = defaultAction) => {
  switch (action.type) {
    // This means the user is starting afresh. Reset everything to default and set the connectionID
    case Actions.SET_BIGQUERY_CONNECTION_ID: {
      if (action.payload.connectionId === state.connectionId) {
        return state;
      }
      return {
        ...defaultBigQueryValue,
        connectionId: action.payload.connectionId,
      };
    }
    case Actions.SET_BIGQUERY_CONNECTION_DETAILS:
      return {
        ...state,
        info: action.payload.info,
        connectionId: objectQuery(action, 'payload', 'connectionId') || state.connectionId,
        error: null,
      };
    case Actions.SET_BIGQUERY_LOADING:
      return {
        ...state,
        loading: true,
      };
    case Actions.SET_BIGQUERY_DATASET_LIST:
      return {
        ...state,
        loading: false,
        datasetList: action.payload.datasetList,
        datasetId: null,
      };
    case Actions.SET_BIGQUERY_TABLE_LIST:
      return {
        ...state,
        loading: false,
        datasetList: [],
        datasetId: action.payload.datasetId,
        tableList: action.payload.tableList,
      };
    case Actions.SET_ERROR:
      return {
        ...state,
        loading: false,
      };
    case Actions.SET_ACTIVEBROWSER:
      return defaultBigQueryValue;
    case Actions.RESET:
      return defaultBigQueryValue;
    default:
      return state;
  }
};

const spanner = (state = defaultSpannerValue, action = defaultAction) => {
  switch (action.type) {
    // This means the user is starting afresh. Reset everything to default and set the connectionID
    case Actions.SET_SPANNER_CONNECTION_ID:
      if (action.payload.connectionId === state.connectionId) {
        return state;
      }
      return {
        ...state,
        connectionId: action.payload.connectionId,
      };
    case Actions.SET_SPANNER_CONNECTION_DETAILS:
      return {
        ...state,
        info: action.payload.info,
        connectionId: objectQuery(action, 'payload', 'connectionId') || state.connectionId,
        error: null,
      };
    case Actions.SET_SPANNER_LOADING:
      return {
        ...state,
        loading: true,
      };
    case Actions.SET_SPANNER_INSTANCE_LIST:
      return {
        ...state,
        loading: false,
        instanceList: action.payload.instanceList,
        instanceId: null,
        databaseId: null,
      };
    case Actions.SET_SPANNER_DATABASE_LIST:
      return {
        ...state,
        loading: false,
        instanceList: [],
        instanceId: action.payload.instanceId,
        databaseList: action.payload.databaseList,
        databaseId: null,
      };
    case Actions.SET_SPANNER_TABLE_LIST:
      return {
        ...state,
        loading: false,
        databaseList: [],
        databaseId: action.payload.databaseId,
        tableList: action.payload.tableList,
      };
    case Actions.SET_ERROR:
      return {
        ...state,
        loading: false,
      };
    case Actions.SET_ACTIVEBROWSER:
      return defaultSpannerValue;
    case Actions.RESET:
      return defaultSpannerValue;
    default:
      return state;
  }
};

const adls = (state = defaultADLSValue, action = defaultAction) => {
  switch (action.type) {
    // This means the user is starting afresh. Reset everything to default and set the connectionID
    case Actions.SET_ADLS_CONNECTION_ID:
      if (action.payload.connectionId === state.connectionId) {
        return state;
      }
      return {
        ...state,
        connectionId: action.payload.connectionId,
      };
    case Actions.SET_ADLS_CONNECTION_DETAILS:
      return {
        ...state,
        info: action.payload.info,
        connectionId: objectQuery(action, 'payload', 'connectionId') || state.connectionId,
        error: null,
      };
    case Actions.SET_ADLS_LOADING:
      return {
        ...state,
        loading: true,
      };
    case Actions.SET_ADLS_PROPERTIES:
      return Object.assign({}, state, {
        info: objectQuery(action, 'payload', 'info') || state.info,
        connectionId: objectQuery(action, 'payload', 'connectionId') || state.connectionId,
        error: null,
        loading: false,
      });
    case Actions.SET_ADLS_FILE_SYSTEM_CONTENTS:
      return {
        ...state,
        loading: false,
        contents: action.payload.contents,
        search: '',
      };
    case Actions.SET_ADLS_FILE_SYSTEM_PATH:
      return {
        ...state,
        path: action.payload.path,
        search: '',
      };
    case Actions.SET_ERROR:
      return {
        ...state,
        loading: false,
      };
    case Actions.SET_ADLS_PREFIX:
      return {
        ...state,
        prefix: action.payload.prefix,
      };
    case Actions.SET_ACTIVEBROWSER:
      return defaultADLSValue;
    case Actions.RESET:
      return defaultADLSValue;
    default:
      return state;
  }
};

const activeBrowser = (state = defaultActiveBrowser, action = defaultAction) => {
  switch (action.type) {
    case Actions.SET_ACTIVEBROWSER:
      return Object.assign({}, state, {
        name: action.payload.name,
      });
    default:
      return state;
  }
};

const error = (state = defaultError, action = defaultAction) => {
  switch (action.type) {
    case Actions.SET_ERROR:
      return action.payload.error;
    default:
      return state;
  }
};

const DataPrepBrowserStore = createStore(
  combineReducers({
    file,
    database,
    kafka,
    activeBrowser,
    s3,
    gcs,
    bigquery,
    spanner,
    adls,
    error,
  }),
  {
    file: defaultFileSystemValue,
    database: defaultDatabaseValue,
    kafka: defaultKafkaValue,
    activeBrowser: defaultActiveBrowser,
    s3: defaultS3Value,
    gcs: defaultGCSValue,
    bigquery: defaultBigQueryValue,
    spanner: defaultSpannerValue,
    adls: defaultADLSValue,
    error: defaultError,
  },
  composeEnhancers('DataPrepBrowserStore')()
);

export default DataPrepBrowserStore;
