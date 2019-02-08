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
const PREFIX = 'features.FileBrowser';

const trimSuffixSlash = (path) => path.replace(/\/\//, '/');

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

const goToPath = (path) => {
  path = trimSuffixSlash(path);
  setFileSystemLoading();
  setFileSystemPath(path);

  const namespace = NamespaceStore.getState().selectedNamespace;

  MyDataPrepApi.explorer({
    context: namespace,
    path,
    hidden: true,
  }).subscribe(
    (res) => {
      DataPrepBrowserStore.dispatch({
        type: BrowserStoreActions.SET_FILE_SYSTEM_CONTENTS,
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

const setFileSystemLoading = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_FILE_SYSTEM_LOADING,
    payload: {
      loading: true,
    },
  });
};

const setFileSystemAsActiveBrowser = (payload) => {
  setActiveBrowser(payload);
  goToPath(payload.path);
};

const setFileSystemPath = (path) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_FILE_SYSTEM_PATH,
    payload: {
      path,
    },
  });
};

const setFileSystemSearch = (search) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_FILE_SYSTEM_SEARCH,
    payload: {
      search,
    },
  });
};

export {
  goToPath,
  trimSuffixSlash,
  setFileSystemLoading,
  setFileSystemAsActiveBrowser,
  setFileSystemSearch,
};
