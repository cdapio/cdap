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
import UploadDataAction from 'services/WizardStores/UploadData/UploadDataActions';
import UploadDataWizardConfig from 'services/WizardConfigs/UploadDataWizardConfig';
import head from 'lodash/head';

const defaultAction = {
  type: '',
  payload: {}
};
const defaultViewData = {
  data: '',
  loading: false,
  filename: '',
  packagename: '',
  packageversion: '',
  __complete: true,
  __skipped: false,
  __error: false
};
const defaultDestinationTypes = [{id: 'streams', value: 'Stream'}];
const defaultSelectDestination = {
  type: defaultDestinationTypes[0].value,
  name: '',
  types: defaultDestinationTypes,
  __complete: false,
  __skipped: true,
  __error: false
};

const defaultInitialState = {
  viewdata: defaultViewData,
  selectdestination: defaultSelectDestination
};

const isNil = (value) => value === null || typeof value === 'undefined' || value === '';
const isComplete = (state, requiredFields) => {
  let emptyFieldsInState = Object.keys(state)
    .filter(fieldName => {
      return isNil(state[fieldName]) && requiredFields.indexOf(fieldName) !== -1;
    });
  return !emptyFieldsInState.length ? true : false;
};
const selectDestinationStepRequiredFields = head(
  UploadDataWizardConfig
    .steps
    .filter(step => step.id === 'selectdestination')
  ).requiredFields;

const viewdata = (state = defaultViewData, action = defaultAction) => {
  switch(action.type) {
    case UploadDataAction.setDefaultData:
      return Object.assign({}, state, {
        data: action.payload.data,
        loading: false
      });
    case UploadDataAction.setFilename:
      return Object.assign({}, state, {
        filename: action.payload.filename
      });
    case UploadDataAction.setPackageInfo:
      return Object.assign({}, state, {
        packagename: action.payload.name,
        packageversion: action.payload.version
      });
    case UploadDataAction.setDefaultDataLoading:
      return Object.assign({}, state, {
        loading: true
      });
    case UploadDataAction.onReset:
      return defaultViewData;
    default:
      return state;
  }
};
const selectdestination = (state = defaultSelectDestination, action = defaultAction) => {
  let stateCopy;
  switch(action.type) {
    case UploadDataAction.setDestinationType:
      stateCopy = Object.assign({}, state, {
        type: action.payload.type
      });
      break;
    case UploadDataAction.setDestinationName:
      stateCopy = Object.assign({}, state, {
        name: action.payload.name
      });
      break;
    case UploadDataAction.onReset:
      return defaultSelectDestination;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __complete: isComplete(stateCopy, selectDestinationStepRequiredFields),
    __skipped: false,
    __error: action.payload.error || false
  });
};
const createStoreWrapper = () => {
  return createStore(
    combineReducers({
      viewdata,
      selectdestination
    }),
    defaultInitialState
  );
};

const CreateStreamStore = createStoreWrapper();
export default CreateStreamStore;
