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
import AddNamespaceActions from 'services/WizardStores/AddNamespace/AddNamespaceActions';
import AddNamespaceWizardConfig from 'services/WizardConfigs/AddNamespaceWizardConfig';
import head from 'lodash/head';
var shortid = require('shortid');

// Defaults
const skippableDefaultState = {
  __complete: true,
  __skipped: true,
  __error: false
};

const nonSkippableDefaultState = {
  __skipped: false,
  __error: false
};

const defaultGeneralState = Object.assign({
  name: '',
  description: '',
  schedulerQueue: ''
}, nonSkippableDefaultState);

const defaultMappingState = Object.assign({
  hdfsDirectory: '',
  hiveDatabaseName: '',
  hbaseNamespace: ''
}, skippableDefaultState);

const defaultSecurityState = Object.assign({
  principal: '',
  keyTab: ''
}, skippableDefaultState);

const defaultPreferencesState = Object.assign({
  keyValues : {
    pairs : [{
      key : '',
      value : '',
      uniqueId : shortid.generate()
    }]
  }
}, skippableDefaultState);

const defaultAction = {
  type: '',
  payload: {},
  uniqueId: shortid.generate()
};

const defaultInitialState = {
  general: defaultGeneralState,
  mapping: defaultMappingState,
  security: defaultSecurityState,
  preferences: defaultPreferencesState
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
  AddNamespaceWizardConfig
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

const general = (state = defaultGeneralState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case AddNamespaceActions.setName:
      stateCopy = Object.assign({}, state, {
        name: action.payload.name
      });
      break;
    case AddNamespaceActions.setDescription:
      stateCopy = Object.assign({}, state, {
        description: action.payload.description
      });
      break;
    case AddNamespaceActions.setSchedulerQueue:
      stateCopy = Object.assign({}, state, {
        schedulerQueue: action.payload.schedulerQueue
      });
      break;
    case AddNamespaceActions.onError:
      return onErrorHandler('general', Object.assign({}, state), action);
    case AddNamespaceActions.onSuccess:
      return onSuccessHandler('general', Object.assign({}, state), action);
    case AddNamespaceActions.onReset:
      return defaultGeneralState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __complete: isComplete(stateCopy, generalStepRequiredFields),
    __error: action.payload.error || false
  });
};

const mapping = (state = defaultMappingState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case AddNamespaceActions.setHDFSDirectory:
      stateCopy = Object.assign({}, state, {
        hdfsDirectory: action.payload.hdfsDirectory
      });
      break;
    case AddNamespaceActions.setHiveDatabaseName:
      stateCopy = Object.assign({}, state, {
        hiveDatabaseName: action.payload.hiveDatabaseName
      });
      break;
    case AddNamespaceActions.setHBaseNamespace:
      stateCopy = Object.assign({}, state, {
        hbaseNamespace: action.payload.hbaseNamespace
      });
      break;
    case AddNamespaceActions.onError:
      return onErrorHandler('mapping', Object.assign({}, state), action);
    case AddNamespaceActions.onSuccess:
      return onSuccessHandler('mapping', Object.assign({}, state), action);
    case AddNamespaceActions.onReset:
      return defaultMappingState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __skipped: false,
    __error: action.payload.error || false
  });
};

const security = (state = defaultSecurityState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case AddNamespaceActions.setPrincipal:
      stateCopy = Object.assign({}, state, {
        principal: action.payload.principal
      });
      break;
    case AddNamespaceActions.setKeytab:
      stateCopy = Object.assign({}, state, {
        keyTab: action.payload.keyTab
      });
      break;
    case AddNamespaceActions.onError:
      return onErrorHandler('security', Object.assign({}, state), action);
    case AddNamespaceActions.onSuccess:
      return onSuccessHandler('security', Object.assign({}, state), action);
    case AddNamespaceActions.onReset:
      return defaultSecurityState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __skipped: false,
    __error: action.payload.error || false
  });
};

const preferences = (state = defaultPreferencesState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case AddNamespaceActions.setPreferences :
      stateCopy = Object.assign({}, state, {
        keyValues : action.payload.keyValues
      });
      break;
    case AddNamespaceActions.onReset:
      return defaultPreferencesState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __skipped: false,
    __error: action.payload.error || false
  });
};

// Store
const createAddNamespaceStore = () => {
  return createStore(
    combineReducers({
      general,
      mapping,
      security,
      preferences
    }),
    defaultInitialState
  );
};

const addNamespaceStore = createAddNamespaceStore();
export default addNamespaceStore;
export {createAddNamespaceStore};
