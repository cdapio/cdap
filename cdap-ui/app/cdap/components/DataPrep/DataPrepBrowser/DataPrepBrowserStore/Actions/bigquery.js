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
import {getCurrentNamespace} from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import {objectQuery} from 'services/helpers';

const setBigQueryAsActiveBrowser = (payload) => {
  let {bigquery} = DataPrepBrowserStore.getState();

  if (bigquery.loading) { return; }

  setActiveBrowser(payload);
  setBigQueryLoading();

  let namespace = getCurrentNamespace();
  let {id} = payload;
  let params = {
    namespace,
    connectionId: id
  };

  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_BIGQUERY_CONNECTION_ID,
    payload: {
      connectionId: id
    }
  });

  MyDataPrepApi.getConnection(params)
    .subscribe((res) => {
      let info = objectQuery(res, 'values', 0);
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_BIGQUERY_CONNECTION_DETAILS,
        payload: {
          info,
          connectionId: id
        }
      });
    });
};

const listBiqQueryDatasets = (connectionId) => {
  setBigQueryLoading();
  let namespace = getCurrentNamespace();
  let params = {
    namespace,
    connectionId
  };

  MyDataPrepApi.bigQueryGetDatasets(params)
    .subscribe((res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_BIGQUERY_DATASET_LIST,
        payload: {
          datasetList: res.values
        }
      });
    });
};

const listBigQueryTables = (connectionId, datasetId) => {
  setBigQueryLoading();
  let namespace = getCurrentNamespace();
  let params = {
    namespace,
    connectionId,
    datasetId
  };

  MyDataPrepApi.bigQueryGetTables(params)
    .subscribe((res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_BIGQUERY_TABLE_LIST,
        payload: {
          datasetId,
          tableList: res.values
        }
      });
    });
};

const setBigQueryLoading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_BIGQUERY_LOADING
  });
};

export {
  setBigQueryAsActiveBrowser,
  listBiqQueryDatasets,
  listBigQueryTables,
  setBigQueryLoading
};
