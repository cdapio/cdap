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

export {
  setGCSAsActiveBrowser,
  setGCSPrefix,
  fetchGCSDetails,
  setGCSLoading,
  setGCSSearch
};
