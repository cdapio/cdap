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
import DataPrepBrowserStore, {Actions as BrowserStoreActions} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import NamespaceStore from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import {objectQuery} from 'services/helpers';

const setDatabaseInfoLoading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_DATABASE_LOADING,
    payload: {
      loading: true
    }
  });
};

const setKafkaInfoLoading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_KAFKA_LOADING,
    payload: {
      loading: true
    }
  });
};

const setDatabaseError = (payload) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_DATABASE_ERROR,
    payload: payload
  });
};

const setKafkaError = (payload) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_KAFKA_ERROR,
    payload: payload
  });
};

const setActiveBrowser = (payload) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_ACTIVEBROWSER,
    payload
  });
};

const setDatabaseAsActiveBrowser = (payload) => {
  setActiveBrowser(payload);
  setDatabaseInfoLoading();

  let namespace = NamespaceStore.getState().selectedNamespace;
  let {id} = payload;
  let params = {
    namespace,
    connectionId: id
  };
  MyDataPrepApi.getConnection(params)
    .subscribe((res) => {
      let info = objectQuery(res, 'values', 0);

      MyDataPrepApi.listTables(params)
        .subscribe((tables) => {
          setDatabaseProperties({
            info,
            tables: tables.values,
            connectionId: params.connectionId,
          });
        }, (err) => {
          setDatabaseError({
            error: err,
            info
          });
        });
    }, (err) => {
      setDatabaseError({
        payload: err
      });
    });
};

const setS3AsActiveBrowser = (payload) => {
  let {s3} = DataPrepBrowserStore.getState();
  if (s3.loading) {
    return;
  }
  setActiveBrowser(payload);
  setS3Loading();
  let namespace = NamespaceStore.getState().selectedNamespace;
  let {id, path} = payload;
  let params = {
    namespace,
    connectionId: id
  };
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_S3_CONNECTION_ID,
    payload: {
      connectionId: id
    }
  });
  if (s3.connectionId !== payload.id) {
    MyDataPrepApi.getConnection(params)
      .subscribe((res) => {
        let info = objectQuery(res, 'values', 0);
        DataPrepBrowserStore.dispatch({
          type: BrowserStoreActions.SET_S3_CONNECTION_DETAILS,
          payload: {
            info,
            connectionId: id
          }
        });
        if (path) {
          setPrefix(path);
        }
      });
  } else {
    if (path) {
      setPrefix(path);
    }
  }
};

const setGCSAsActiveBrowser = (payload) => {
  let {gcs} = DataPrepBrowserStore.getState();

  if (gcs.loading) { return; }

  setActiveBrowser(payload);
  setGCSLoading();

  let namespace = NamespaceStore.getState().selectedNamespace;
  let {id, path} = payload;
  let params = {
    namespace,
    connectionId: id
  };

  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_GCS_CONNECTION_ID,
    payload: {
      connectionId: id
    }
  });
  if (gcs.connectionId !== payload.id) {
    MyDataPrepApi.getConnection(params)
      .subscribe((res) => {
        let info = objectQuery(res, 'values', 0);
        DataPrepBrowserStore.dispatch({
          type: BrowserStoreActions.SET_GCS_CONNECTION_DETAILS,
          payload: {
            info,
            connectionId: id
          }
        });
        if (path) {
          setGCSPrefix(path);
        }
      });
  } else {
    if (path) {
      setGCSPrefix(path);
    }
  }
};

const setGCSPrefix = (prefix) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_GCS_PREFIX,
    payload: {
      prefix
    }
  });
  fetchGCSDetails(prefix);
};

const fetchGCSDetails = (path = '') => {
  let { connectionId, loading} = DataPrepBrowserStore.getState().gcs;
  if (loading) {
    return;
  }
  setGCSLoading();
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  let params = {
    namespace,
    connectionId
  };
  if (path) {
    params = {...params, path};
  }
  MyDataPrepApi.exploreGCSBucketDetails(params)
    .subscribe(
      res => {
        DataPrepBrowserStore.dispatch({
          type: BrowserStoreActions.SET_GCS_ACTIVE_BUCKET_DETAILS,
          payload: {
            activeBucketDetails: res.values
          }
        });
      }
    );
};

const setGCSLoading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_GCS_LOADING
  });
};

const setGCSSearch = (search) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_GCS_SEARCH,
    payload: { search }
  });
};

const setPrefix = (prefix) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_S3_PREFIX,
    payload: {
      prefix
    }
  });
  fetchBucketDetails(prefix);
};

const fetchBucketDetails = (path = '') => {
  let { connectionId, loading} = DataPrepBrowserStore.getState().s3;
  if (loading) {
    return;
  }
  setS3Loading();
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  let params = {
    namespace,
    connectionId
  };
  if (path) {
    params = {...params, path};
  }
  MyDataPrepApi
    .exploreBucketDetails(params)
    .subscribe(
      res => {
        DataPrepBrowserStore.dispatch({
          type: BrowserStoreActions.SET_S3_ACTIVE_BUCKET_DETAILS,
          payload: {
            activeBucketDetails: res.values
          }
        });
      }
    );
};

const setS3Loading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_S3_LOADING
  });
};

const setS3Search = (search) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_S3_SEARCH,
    payload: {search}
  });
};

const setKafkaAsActiveBrowser = (payload) => {
  setActiveBrowser(payload);
  setKafkaInfoLoading();

  let namespace = NamespaceStore.getState().selectedNamespace;
  let {id} = payload;
  let params = {
    namespace,
    connectionId: id
  };

  MyDataPrepApi.getConnection(params)
    .subscribe((res) => {
      let info = objectQuery(res, 'values', 0);

      MyDataPrepApi.listTopics({namespace}, info)
        .subscribe((topics) => {
          setKafkaProperties({
            info,
            topics: topics.values,
            connectionId: params.connectionId
          });
        }, (err) => {
          setKafkaError({
            error: err,
            info
          });
        });

    }, (err) => {
      setKafkaError({
        payload: err
      });
    });
};

const setDatabaseProperties = (payload) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_DATABASE_PROPERTIES,
    payload
  });
};

const setKafkaProperties = (payload) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_KAFKA_PROPERTIES,
    payload
  });
};

export {
  setActiveBrowser,
  setS3AsActiveBrowser,
  setS3Search,
  setGCSAsActiveBrowser,
  setGCSSearch,
  setGCSLoading,
  setGCSPrefix,
  fetchGCSDetails,
  setDatabaseProperties,
  setDatabaseAsActiveBrowser,
  setKafkaAsActiveBrowser,
  setPrefix,
  fetchBucketDetails,
  setS3Loading
};
