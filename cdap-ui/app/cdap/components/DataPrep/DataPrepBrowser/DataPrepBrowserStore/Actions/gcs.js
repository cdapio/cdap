/*
 * Copyright © 2018-2019 Cask Data, Inc.
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

import { setActiveBrowser, setError } from './commons';
import DataPrepBrowserStore, {
  Actions as BrowserStoreActions,
} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import NamespaceStore from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';

const setGCSAsActiveBrowser = (payload) => {
  let { gcs, activeBrowser } = DataPrepBrowserStore.getState();

  if (activeBrowser.name !== payload.name) {
    setActiveBrowser(payload);
  }

  let { id: connectionId, path } = payload;

  if (gcs.connectionId === connectionId) {
    if (path && path !== gcs.prefix) {
      setGCSPrefix(path);
    }
    return;
  }

  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_GCS_CONNECTION_ID,
    payload: {
      connectionId,
    },
  });

  setGCSLoading();

  let namespace = NamespaceStore.getState().selectedNamespace;
  let params = {
    context: namespace,
    connectionId,
  };

  MyDataPrepApi.getConnection(params).subscribe(
    (res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_GCS_CONNECTION_DETAILS,
        payload: {
          info: res,
          connectionId,
        },
      });
      if (path) {
        setGCSPrefix(path);
      } else {
        fetchGCSDetails();
      }
    },
    (err) => {
      setError(err);
    }
  );
};

const setGCSPrefix = (prefix) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_GCS_PREFIX,
    payload: {
      prefix,
    },
  });
  fetchGCSDetails(prefix);
};

const fetchGCSDetails = (path = '') => {
  let { connectionId, loading } = DataPrepBrowserStore.getState().gcs;
  if (!loading) {
    setGCSLoading();
  }
  let { selectedNamespace: namespace } = NamespaceStore.getState();
  let params = {
    context: namespace,
    connectionId,
  };
  if (path) {
    params = { ...params, path };
  }
  MyDataPrepApi.exploreGCSBucketDetails(params).subscribe(
    (res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_GCS_ACTIVE_BUCKET_DETAILS,
        payload: {
          activeBucketDetails: res.values,
          truncated: res.truncated === 'true' || false,
        },
      });
    },
    (err) => {
      setError(err);
    }
  );
};

const setGCSLoading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_GCS_LOADING,
  });
};

const setGCSSearch = (search) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_GCS_SEARCH,
    payload: { search },
  });
};

export { setGCSAsActiveBrowser, setGCSPrefix, fetchGCSDetails, setGCSLoading, setGCSSearch };
