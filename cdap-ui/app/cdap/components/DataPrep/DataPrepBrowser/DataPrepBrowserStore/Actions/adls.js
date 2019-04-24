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
import { convertBytesToHumanReadable, HUMANREADABLESTORAGE_NODECIMAL } from 'services/helpers';
import uuidV4 from 'uuid/v4';
import moment from 'moment';
import T from 'i18n-react';
import { objectQuery } from 'services/helpers';

const PREFIX = 'features.ADLSBrowser';
const trimSuffixSlash = (path) => path.replace(/\/\//, '/');

const setAdlsAsActiveBrowser = (payload) => {
  let { adls, activeBrowser } = DataPrepBrowserStore.getState();

  if (activeBrowser.name !== payload.name) {
    setActiveBrowser(payload);
  }

  let { id: connectionId, path } = payload;

  if (adls.connectionId === connectionId) {
    if (path && path !== adls.prefix) {
      setADLSPrefix(path);
    }
    return;
  }

  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_ADLS_CONNECTION_ID,
    payload: {
      connectionId,
    },
  });

  setADLSLoading();

  let namespace = NamespaceStore.getState().selectedNamespace;
  let params = {
    context: namespace,
    connectionId,
  };

  MyDataPrepApi.getConnection(params).subscribe(
    (res) => {
      let info = objectQuery(res, 'values', 0);
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_ADLS_CONNECTION_DETAILS,
        payload: {
          info,
          connectionId,
        },
      });
      if (path) {
        setADLSPrefix(path);
      } else {
        goToADLSfilePath();
      }
    },
    (err) => {
      setError(err);
    }
  );
};

const setADLSPrefix = (prefix) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_ADLS_PREFIX,
    payload: {
      prefix,
    },
  });
  goToADLSfilePath(prefix);
};

const goToADLSfilePath = (path = '/') => {
  let { connectionId, loading } = DataPrepBrowserStore.getState().adls;
  if (!loading) {
    setADLSLoading();
  }
  let { selectedNamespace: namespace } = NamespaceStore.getState();
  if (path) {
    path = trimSuffixSlash(path);
    setADLSFileSystemLoading();
    setADLSFileSystemPath(path);
  }

  MyDataPrepApi.adlsFileExplorer({
    context: namespace,
    path,
    connectionId,
  }).subscribe(
    (res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_ADLS_FILE_SYSTEM_CONTENTS,
        payload: {
          contents: formatResponse(res.values),
        },
      });
    },
    (err) => {
      setError(err);
    }
  );
};

const formatResponse = (contents) => {
  return contents.map((content) => {
    content.uniqueId = uuidV4();
    content['last-modified'] = moment(content['last-modified']).format('MM/DD/YY HH:mm');
    content.displaySize = convertBytesToHumanReadable(
      content.size,
      HUMANREADABLESTORAGE_NODECIMAL,
      true
    );

    if (content.directory) {
      content.type = T.translate(`${PREFIX}.directory`);
    }
    content.type = content.type === 'UNKNOWN' ? '--' : content.type;

    return content;
  });
};

const setADLSLoading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_ADLS_LOADING,
  });
};

const setADLSFileSystemLoading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_ADLS_FILE_SYSTEM_LOADING,
    payload: {
      loading: true,
    },
  });
};

const setADLSFileSystemPath = (path) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_ADLS_FILE_SYSTEM_PATH,
    payload: {
      path,
    },
  });
};

const setADLSFileSystemSearch = (search) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_ADLS_FILE_SYSTEM_SEARCH,
    payload: {
      search,
    },
  });
};
export {
  trimSuffixSlash,
  setAdlsAsActiveBrowser,
  setADLSPrefix,
  goToADLSfilePath,
  setADLSLoading,
  setADLSFileSystemLoading,
  setADLSFileSystemSearch,
};
