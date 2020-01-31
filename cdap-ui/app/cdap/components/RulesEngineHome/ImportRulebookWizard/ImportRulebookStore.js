/*
 * Copyright © 2017 Cask Data, Inc.
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

import { createStore, combineReducers } from 'redux';
import { defaultAction } from 'services/helpers';

const DEFAULTUPLOADSTEP = {
  file: {
    name: '',
    contents: '',
  },
  __complete: false,
};

const DEFAULTSTATE = {
  upload: DEFAULTUPLOADSTEP,
};

const IMPORTRULEBOOKACTIONS = {
  UPLOADFILE: 'UPLOADFILE',
};

const upload = (state = DEFAULTUPLOADSTEP, action = defaultAction) => {
  switch (action.type) {
    case IMPORTRULEBOOKACTIONS.UPLOADFILE:
      return Object.assign({}, state, {
        file: action.payload.file,
        __complete: true,
      });
    default:
      return state;
  }
};

const ImportRulebookStore = createStore(
  combineReducers({
    upload,
  }),
  DEFAULTSTATE,
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export { IMPORTRULEBOOKACTIONS };
export default ImportRulebookStore;
