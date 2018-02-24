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

import {setActiveBrowser} from './commons';
import DataPrepBrowserStore, {Actions as BrowserStoreActions} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import NamespaceStore from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import {objectQuery} from 'services/helpers';

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

export {
  setS3AsActiveBrowser,
  setPrefix,
  fetchBucketDetails,
  setS3Loading,
  setS3Search
};
