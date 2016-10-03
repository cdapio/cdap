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
import CreateStreamActions from 'services/WizardStores/CreateStream/CreateStreamActions';
import CreateStreamWizardConfig from 'services/WizardConfigs/CreateStreamWizardConfig';
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
}, defaultState, { __complete: true });
const defaultThresholdState = Object.assign({
  value: 1024
}, defaultState, { __complete: true });
const defaultAction = {
  type: '',
  payload: {}
};
const defaultInitialState = {
  general: defaultGeneralState,
  schema: cloneDeep(defaultSchemaState),
  threshold: defaultThresholdState
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
  CreateStreamWizardConfig
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
  switch(action.type) {
    case CreateStreamActions.setName:
      stateCopy = Object.assign({}, state, {
        name: action.payload.name
      });
      break;
    case CreateStreamActions.setDescription:
      stateCopy = Object.assign({}, state, {
        description: action.payload.description
      });
      break;
    case CreateStreamActions.setTTL:
      stateCopy = Object.assign({}, state, {
        ttl: action.payload.ttl
      });
      break;
    case CreateStreamActions.onError:
      return onErrorHandler('general', Object.assign({}, state), action);
    case CreateStreamActions.onSuccess:
      return onSuccessHandler('general', Object.assign({}, state), action);
    case CreateStreamActions.onReset:
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
  switch(action.type) {
    case CreateStreamActions.setThreshold:
      stateCopy = Object.assign({}, state, {
        value: action.payload.threshold
      });
      break;
    case CreateStreamActions.onError:
      return onErrorHandler('threshold', Object.assign({}, state), action);
    case CreateStreamActions.onSuccess:
      return onSuccessHandler('threshold', Object.assign({}, state), action);
    case CreateStreamActions.onReset:
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
  switch(action.type) {
    case CreateStreamActions.setSchemaFormat:
      stateCopy = Object.assign({}, state, {
        format: action.payload.format
      });
      break;
    case CreateStreamActions.setSchema:
      stateCopy = Object.assign({}, state, {
        value: action.payload.schema,
      });
      break;
    case CreateStreamActions.onError:
      return onErrorHandler('schema', Object.assign({}, state), action);
    case CreateStreamActions.onSuccess:
      return onSuccessHandler('schema', Object.assign({}, state), action);
    case CreateStreamActions.onReset:
      return cloneDeep(defaultSchemaState);
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
      threshold
    }),
    defaultInitialState
  );
};

const CreateStreamStore = createStoreWrapper();
export default CreateStreamStore;
export {createStoreWrapper, defaultSchemaFormats};
