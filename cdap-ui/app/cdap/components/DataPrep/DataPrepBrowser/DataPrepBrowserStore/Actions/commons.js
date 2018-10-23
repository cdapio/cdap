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

import DataPrepBrowserStore, {
  Actions as BrowserStoreActions,
} from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore';

const setActiveBrowser = (payload) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_ACTIVEBROWSER,
    payload,
  });
};

const setError = (error = null) => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.SET_ERROR,
    payload: {
      error,
    },
  });
};

const reset = () => {
  DataPrepBrowserStore.dispatch({
    type: BrowserStoreActions.RESET,
  });
};

export { setActiveBrowser, setError, reset };
