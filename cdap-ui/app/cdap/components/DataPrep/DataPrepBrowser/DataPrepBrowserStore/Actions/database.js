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

const setDatabaseInfoLoading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_DATABASE_LOADING,
    payload: {
      loading: true,
    },
  });
};

const setDatabaseAsActiveBrowser = (payload) => {
  let { database, activeBrowser } = DataPrepBrowserStore.getState();

  if (activeBrowser.name !== payload.name) {
    setActiveBrowser(payload);
  }

  let { id: connectionId } = payload;

  if (database.connectionId === connectionId) {
    return;
  }

  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_DATABASE_CONNECTION_ID,
    payload: {
      connectionId,
    },
  });

  setDatabaseInfoLoading();

  let namespace = NamespaceStore.getState().selectedNamespace;

  let params = {
    context: namespace,
    connectionId,
  };
  MyDataPrepApi.getConnection(params).subscribe(
    (res) => {
      MyDataPrepApi.listTables(params).subscribe(
        (tables) => {
          setDatabaseProperties({
            info: res,
            tables: tables.values,
            connectionId: params.connectionId,
          });
        },
        (err) => {
          setError(err);
        }
      );
    },
    (err) => {
      setError(err);
    }
  );
};

const setDatabaseProperties = (payload) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_DATABASE_PROPERTIES,
    payload,
  });
};

export { setDatabaseInfoLoading, setDatabaseAsActiveBrowser, setDatabaseProperties };
