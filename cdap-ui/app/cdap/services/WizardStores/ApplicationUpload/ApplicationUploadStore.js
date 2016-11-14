/*
 * Copyright © 2016 Cask Data, Inc.
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

import {combineReducers, createStore} from 'redux';
import ApplicationUploadActions from 'services/WizardStores/ApplicationUpload/ApplicationUploadActions';

const defaultAction = {
  type: '',
  payload: {}
};
const defaultState = {
  __complete: false,
  __skipped: false,
  __error: false
};
const defaultUploadFileState = Object.assign({
  file: ''
}, defaultState);
const defaultInitialState = {
  uploadFile: defaultUploadFileState
};
const uploadFile = (state = defaultUploadFileState, action = defaultAction) => {
  switch (action.type) {
    case ApplicationUploadActions.UPLOAD_JAR:
      return Object.assign({}, state, {
        file: action.payload.file
      });
    case ApplicationUploadActions.onReset:
      return defaultUploadFileState;
    default:
      return state;
  }
};
const ApplicationUploadStoreWrapper = () => {
  return createStore(
    combineReducers({
      uploadFile
    }),
    defaultInitialState
  );
};

const ApplicationUploadStore = ApplicationUploadStoreWrapper();
export default ApplicationUploadStore;
