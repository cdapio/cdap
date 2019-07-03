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
import { combineReducers, createStore } from 'redux';
import ArtifactUploadActions from 'services/WizardStores/ArtifactUpload/ArtifactUploadActions';
import ArtifactUploadWizardConfig from 'services/WizardConfigs/ArtifactUploadWizardConfig';
import head from 'lodash/head';

const defaultAction = {
  type: '',
  payload: {},
};
const defaultState = {
  __complete: false,
  __skipped: false,
  __error: false,
};

const defaultConfigureState = Object.assign(
  {
    name: '',
    type: '',
    description: '',
    classname: '',
    version: '',
    parentArtifact: [
      'system:cdap-data-pipeline[3.0.0,10.0.0]',
      'system:cdap-data-streams[3.0.0,10.0.0]',
      'system:cdap-etl-batch[3.0.0,10.0.0]',
    ],
  },
  defaultState
);

const defaultUploadState = Object.assign(
  {
    file: [],
  },
  defaultState
);

const defaultInitialState = {
  upload: defaultUploadState,
  configure: defaultConfigureState,
};

const isNil = (value) => value === null || typeof value === 'undefined' || value === '';
const isComplete = (state, requiredFields) => {
  let emptyFieldsInState = Object.keys(state).filter((fieldName) => {
    return isNil(state[fieldName]) && requiredFields.indexOf(fieldName) !== -1;
  });
  return !emptyFieldsInState.length ? true : false;
};

const upload = (state = defaultUploadState, action = defaultAction) => {
  switch (action.type) {
    case ArtifactUploadActions.setFilePath:
      return Object.assign({}, state, {
        file: action.payload.file,
        __complete: true,
      });
    case ArtifactUploadActions.onReset:
      return defaultUploadState;
    default:
      return state;
  }
};

const configure = (state = defaultConfigureState, action = defaultAction) => {
  let stateCopy;
  let configurationStepRequiredFields = [];
  if (ArtifactUploadWizardConfig) {
    configurationStepRequiredFields = head(
      ArtifactUploadWizardConfig.steps.filter((step) => step.id === 'configuration')
    ).requiredFields;
  }
  switch (action.type) {
    case ArtifactUploadActions.setName:
      stateCopy = Object.assign({}, state, {
        name: action.payload.name,
      });
      break;
    case ArtifactUploadActions.setDescription:
      stateCopy = Object.assign({}, state, {
        description: action.payload.description,
      });
      break;
    case ArtifactUploadActions.setClassname:
      stateCopy = Object.assign({}, state, {
        classname: action.payload.classname,
      });
      break;
    case ArtifactUploadActions.setType:
      stateCopy = Object.assign({}, state, {
        type: action.payload.type,
      });
      break;
    case ArtifactUploadActions.setVersion:
      stateCopy = Object.assign({}, state, {
        version: action.payload.version,
      });
      break;
    case ArtifactUploadActions.setNameAndClass:
      stateCopy = Object.assign({}, state, {
        name: action.payload.name,
        classname: action.payload.classname,
      });
      break;
    case ArtifactUploadActions.onReset:
      return defaultConfigureState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __complete: isComplete(stateCopy, configurationStepRequiredFields),
  });
};

const ArtifactUploadStoreWrapper = () => {
  return createStore(
    combineReducers({
      upload,
      configure,
    }),
    defaultInitialState
  );
};

const ArtifactUploadStore = ArtifactUploadStoreWrapper();
export default ArtifactUploadStore;
