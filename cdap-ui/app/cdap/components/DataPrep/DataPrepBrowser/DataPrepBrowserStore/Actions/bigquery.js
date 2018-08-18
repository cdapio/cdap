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

import {setActiveBrowser, setError} from './commons';
import DataPrepBrowserStore, {Actions as BrowserStoreActions} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';
import {getCurrentNamespace} from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import {objectQuery} from 'services/helpers';

const setBigQueryAsActiveBrowser = (payload) => {
  let {bigquery} = DataPrepBrowserStore.getState();

  /*
    TL;DR - This is needed to prevent UI from making a redundant datasets list call even though the user clicked on one single dataset.

    Detailed version:

    Scenario that warranted this change.
    1. User goes to /connections/browser
    2. Clicks on a big query connection by clicking on the left panel
    3. The datasetList will be empty and we go and fetch the list of datasets from bigquery
    4. User clicks on a dataset
    5. If the routing is enabled (meaning if the user clicks on react side of this) it is a link.
    6. If its a link the event propagates up till the DataPrepConnections where we handle /connections/bigquery/:bigqueryid render method
    <Route
      path={`${BASEPATH}/bigquery/:bigQueryId`}
      render={(match) => {
        let id  = match.match.params.bigQueryId;
        setBigQueryAsActiveBrowser({name: 'bigquery', id});
        return (
          <DataPrepBrowser
            match={match}
            location={location}
            toggle={this.toggleSidePanel}
            onWorkspaceCreate={this.onUploadSuccess}
          />
        );
      }}
    />
    Here in the render method we set bigquery as the default browser which will again call this
    7. Without this check we get the list of datasets AGAIN even though the user clicked on A dataset
    8. In the /connections/bigquery/:bigqueryid/datasets/:datasetid which renders TableList makes the list of tables call
    9. This is redundant.

    One other reason we want to avoid redundant requests is to avoid race condition between TableList and dataset list.
    One overwrites the other and hence the path gets screwed in the breadcrumb while browsing datasets
    (one overwrites dataestId and the other overwrites tableList)

    This will prevent the user from clicking on the connection in the left panel to refresh the contents.
  */
  if (
    bigquery.loading ||
    bigquery.connectionId === payload.id
  ) { return; }

  let {id} = payload;
  setActiveBrowser(payload);
  setBigQueryLoading();
  listBiqQueryDatasets(id);

  let namespace = getCurrentNamespace();
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
    }, (err) => {
      setError(err);
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
    }, (err) => {
      setError(err);
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
    }, (err) => {
      setError(err);
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
