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

const setDatabaseInfoLoading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_DATABASE_LOADING,
    payload: {
      loading: true
    }
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

const setDatabaseProperties = (payload) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_DATABASE_PROPERTIES,
    payload
  });
};

const setDatabaseError = (payload) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_DATABASE_ERROR,
    payload: payload
  });
};

export {
  setDatabaseInfoLoading,
  setDatabaseAsActiveBrowser,
  setDatabaseProperties,
  setDatabaseError
};
