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
import { getCurrentNamespace } from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';

const setSpannerAsActiveBrowser = (payload, getInstances = false) => {
  let { spanner, activeBrowser } = DataPrepBrowserStore.getState();

  if (activeBrowser.name !== payload.name) {
    setActiveBrowser(payload);
  }

  let { id: connectionId } = payload;

  if (spanner.connectionId === connectionId) {
    return;
  }

  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_SPANNER_CONNECTION_ID,
    payload: {
      connectionId,
    },
  });

  setSpannerLoading();

  let namespace = getCurrentNamespace();
  let params = {
    context: namespace,
    connectionId,
  };

  MyDataPrepApi.getConnection(params).subscribe(
    (res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_SPANNER_CONNECTION_DETAILS,
        payload: {
          info: res,
          connectionId,
        },
      });
      if (getInstances) {
        listSpannerInstances(connectionId);
      }
    },
    (err) => {
      setError(err);
    }
  );
};

const listSpannerInstances = (connectionId) => {
  setSpannerLoading();
  let namespace = getCurrentNamespace();
  let params = {
    context: namespace,
    connectionId,
  };

  MyDataPrepApi.spannerGetInstances(params).subscribe(
    (res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_SPANNER_INSTANCE_LIST,
        payload: {
          instanceList: res.values,
        },
      });
    },
    (err) => {
      setError(err);
    }
  );
};

const listSpannerDatabases = (connectionId, instanceId) => {
  setSpannerLoading();
  let namespace = getCurrentNamespace();
  let params = {
    context: namespace,
    connectionId,
    instanceId,
  };

  MyDataPrepApi.spannerGetDatabases(params).subscribe(
    (res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_SPANNER_DATABASE_LIST,
        payload: {
          instanceId,
          databaseList: res.values,
        },
      });
    },
    (err) => {
      setError(err);
    }
  );
};

const listSpannerTables = (connectionId, instanceId, databaseId) => {
  setSpannerLoading();
  let namespace = getCurrentNamespace();
  let params = {
    context: namespace,
    connectionId,
    instanceId,
    databaseId,
  };

  MyDataPrepApi.spannerGetTables(params).subscribe(
    (res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_SPANNER_TABLE_LIST,
        payload: {
          databaseId,
          tableList: res.values,
        },
      });
    },
    (err) => {
      setError(err);
    }
  );
};

const setSpannerLoading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_SPANNER_LOADING,
  });
};

export {
  setSpannerAsActiveBrowser,
  listSpannerInstances,
  listSpannerDatabases,
  listSpannerTables,
  setSpannerLoading,
};
