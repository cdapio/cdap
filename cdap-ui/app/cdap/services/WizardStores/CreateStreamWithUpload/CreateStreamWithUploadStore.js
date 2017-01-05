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
import CreateStreamWithUploadAction from 'services/WizardStores/CreateStreamWithUpload/CreateStreamWithUploadActions';
import CreateStreamUploadWizardConfig from 'services/WizardConfigs/CreateStreamWithUploadWizardConfig';
import head from 'lodash/head';
import cloneDeep from 'lodash/cloneDeep';

// Defaults
const defaultState = {
  __complete: false,
  __skipped: true,
  __error: false
};
const defaultGeneralState = Object.assign({
  name: '',
  description: '',
  ttl: ''
}, defaultState, {__skipped: false});
const defaultRow = {
  "name": "body",
  "type": "string",
  "nullable": false
};
const defaultSchema = {
  "type":"record",
  "name":"etlSchemaBody",
  "fields":[defaultRow]
};
const defaultSchemaFormats = ['', 'text', 'csv', 'syslog'];
const defaultSchemaState = Object.assign({
  format: defaultSchemaFormats[1],
  value: cloneDeep(defaultSchema)
}, defaultState, {__complete: true, __skipped: true});
const defaultThresholdState = Object.assign({
  value: 1024
}, defaultState, {__complete: true, __skipped: true});
const defaultViewData = Object.assign({
  data: '',
  loading: false,
  filename: '',
}, defaultState, {__skipped: true, __complete: true});

const defaultAction = {
  type: '',
  payload: {}
};
const defaultInitialState = {
  general: defaultGeneralState,
  schema: cloneDeep(defaultSchemaState),
  threshold: defaultThresholdState,
  upload: defaultViewData
};
// Utilities. FIXME: Move to a common place?
const isNil = (value) => value === null || typeof value === 'undefined' || value === '';
const isComplete = (state, requiredFields) => {
  let emptyFieldsInState = Object.keys(state)
    .filter(fieldName => {
      return isNil(state[fieldName]) && requiredFields.indexOf(fieldName) !== -1;
    });
  return !emptyFieldsInState.length ? true : false;
};
const generalStepRequiredFields = head(
  CreateStreamUploadWizardConfig
    .steps
    .filter(step => step.id === 'general')
  ).requiredFields;
const onErrorHandler = (reducerId, stateCopy, action) => {
  stateCopy = Object.assign({}, stateCopy);
  if (action.payload.id === reducerId) {
    stateCopy.__error = action.payload.error;
  }
  return stateCopy;
};
const onSuccessHandler = (reducerId, stateCopy, action) => {
  stateCopy = Object.assign({}, stateCopy, action);
  if (action.payload.id === 'general') {
    stateCopy.__complete = action.payload.res;
  }
  return stateCopy;
};

// Reducers
const general = (state = defaultGeneralState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case CreateStreamWithUploadAction.setName:
      stateCopy = Object.assign({}, state, {
        name: action.payload.name
      });
      break;
    case CreateStreamWithUploadAction.setDescription:
      stateCopy = Object.assign({}, state, {
        description: action.payload.description
      });
      break;
    case CreateStreamWithUploadAction.setTTL:
      stateCopy = Object.assign({}, state, {
        ttl: action.payload.ttl
      });
      break;
    case CreateStreamWithUploadAction.onError:
      return onErrorHandler('general', Object.assign({}, state), action);
    case CreateStreamWithUploadAction.onSuccess:
      return onSuccessHandler('general', Object.assign({}, state), action);
    case CreateStreamWithUploadAction.onReset:
      return defaultGeneralState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __complete: isComplete(stateCopy, generalStepRequiredFields),
    __error: action.payload.error || false
  });
};
const threshold = (state = defaultThresholdState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case CreateStreamWithUploadAction.setThreshold:
      stateCopy = Object.assign({}, state, {
        value: action.payload.threshold
      });
      break;
    case CreateStreamWithUploadAction.onError:
      return onErrorHandler('threshold', Object.assign({}, state), action);
    case CreateStreamWithUploadAction.onSuccess:
      return onSuccessHandler('threshold', Object.assign({}, state), action);
    case CreateStreamWithUploadAction.onReset:
      return defaultThresholdState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __skipped: false,
    __error: action.payload.error || false
  });
};
const schema = (state = defaultSchemaState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case CreateStreamWithUploadAction.setSchemaFormat:
      stateCopy = Object.assign({}, state, {
        format: action.payload.format
      });
      break;
    case CreateStreamWithUploadAction.setSchema:
      stateCopy = Object.assign({}, state, {
        value: action.payload.schema,
      });
      break;
    case CreateStreamWithUploadAction.onError:
      return onErrorHandler('schema', Object.assign({}, state), action);
    case CreateStreamWithUploadAction.onSuccess:
      return onSuccessHandler('schema', Object.assign({}, state), action);
    case CreateStreamWithUploadAction.onReset:
      return cloneDeep(defaultSchemaState);
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __skipped: false,
    __error: action.payload.error || false
  });
};
const upload = (state = defaultViewData, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case CreateStreamWithUploadAction.setData:
      stateCopy = Object.assign({}, state, {
        data: action.payload.data,
        filename: action.payload.filename,
        loading: false
      });
      break;
    case CreateStreamWithUploadAction.onReset:
      return defaultViewData;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __skipped: false,
    __error: action.payload.error || false
  });
};
// Store
const createStoreWrapper = () => {
  return createStore(
    combineReducers({
      general,
      schema,
      threshold,
      upload
    }),
    defaultInitialState
  );
};

const CreateStreamWithUploadStore = createStoreWrapper();
export default CreateStreamWithUploadStore;
export {createStoreWrapper, defaultSchemaFormats};
