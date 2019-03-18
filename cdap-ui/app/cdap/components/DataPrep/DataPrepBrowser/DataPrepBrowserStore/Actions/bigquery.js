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

const setBigQueryAsActiveBrowser = (payload, getDatasets = false) => {
  let { bigquery, activeBrowser } = DataPrepBrowserStore.getState();

  if (activeBrowser.name !== payload.name) {
    setActiveBrowser(payload);
  }

  let { id: connectionId } = payload;

  if (bigquery.connectionId === connectionId) {
    return;
  }

  setBigQueryLoading();

  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_BIGQUERY_CONNECTION_ID,
    payload: {
      connectionId,
    },
  });

  let namespace = getCurrentNamespace();
  let params = {
    context: namespace,
    connectionId,
  };

  MyDataPrepApi.getConnection(params).subscribe(
    (res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_BIGQUERY_CONNECTION_DETAILS,
        payload: {
          info: res,
          connectionId,
        },
      });
      if (getDatasets) {
        listBiqQueryDatasets(connectionId);
      }
    },
    (err) => {
      setError(err);
    }
  );
};

const listBiqQueryDatasets = (connectionId) => {
  setBigQueryLoading();
  let namespace = getCurrentNamespace();
  let params = {
    context: namespace,
    connectionId,
  };

  MyDataPrepApi.bigQueryGetDatasets(params).subscribe(
    (res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_BIGQUERY_DATASET_LIST,
        payload: {
          datasetList: res.values,
        },
      });
    },
    (err) => {
      setError(err);
    }
  );
};

const listBigQueryTables = (connectionId, datasetId) => {
  setBigQueryLoading();
  let namespace = getCurrentNamespace();
  let params = {
    context: namespace,
    connectionId,
    datasetId,
  };

  MyDataPrepApi.bigQueryGetTables(params).subscribe(
    (res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_BIGQUERY_TABLE_LIST,
        payload: {
          datasetId,
          tableList: res.values,
        },
      });
    },
    (err) => {
      setError(err);
    }
  );
};

const setBigQueryLoading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_BIGQUERY_LOADING,
  });
};

export { setBigQueryAsActiveBrowser, listBiqQueryDatasets, listBigQueryTables, setBigQueryLoading };
