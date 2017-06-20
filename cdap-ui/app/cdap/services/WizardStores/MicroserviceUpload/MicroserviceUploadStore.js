/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License s
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
import MicroserviceUploadAction from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';
import MicroserviceUploadWizardConfig from 'services/WizardConfigs/MicroserviceUploadWizardConfig';
import {getArtifactNameAndVersion} from 'services/helpers';
import head from 'lodash/head';
import T from 'i18n-react';

// Defaults
const defaultState = {
  __complete: false,
  __skipped: false,
  __error: false
};
const defaultGeneralState = Object.assign({
  instanceName: '',
  description: '',
  version: 1,
  microserviceName: ''
}, defaultState);

const defaultJarState = Object.assign({}, {
  contents: '',
  fileMetadataObj: {}
}, defaultState);

const defaultJsonState = Object.assign({}, {
  contents: '',
  properties: {},
  artifactExtends: '',
  artifactPlugins: []
}, defaultState);

const defaultUploadState = Object.assign({
  jar: defaultJarState,
  json: defaultJsonState
});

const defaultConfigureState = Object.assign({
  instances: 1,
  vcores: 1,
  memory: 512,
  ethreshold: 100
}, defaultState, { __complete: true });

const defaultPropertiesState = Object.assign({
  "dataset" : "device_fences",
  "cache-expiration-in-seconds": 300,
  "out-bound-stream" : "pubnubIngress"
}, defaultState, { __complete: true });

const defaultEndpointsState = Object.assign({
  fetch : 100,
  in: ["ms-ericsson-geo-events"]
}, defaultState, { __complete: true });

const defaultAction = {
  type: '',
  payload: {}
};
const defaultInitialState = {
  general: defaultGeneralState,
  upload: defaultUploadState,
  configure: defaultConfigureState,
  properties: defaultPropertiesState,
  endpoints: defaultEndpointsState
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
  MicroserviceUploadWizardConfig
    .steps
    .filter(step => step.id === 'general')
  ).requiredFields;
const configureStepRequiredFields = head(
  MicroserviceUploadWizardConfig
    .steps
    .filter(step => step.id === 'configure')
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
    case MicroserviceUploadAction.setInstanceName:
      stateCopy = Object.assign({}, state, {
        instanceName: action.payload.instanceName
      });
      break;
    case MicroserviceUploadAction.setDescription:
      stateCopy = Object.assign({}, state, {
        description: action.payload.description
      });
      break;
    case MicroserviceUploadAction.setVersion:
      stateCopy = Object.assign({}, state, {
        version: action.payload.version
      });
      break;
    case MicroserviceUploadAction.setMicroserviceName:
      stateCopy = Object.assign({}, state, {
        microserviceName: action.payload.microserviceName
      });
      break;
    case MicroserviceUploadAction.onError:
      return onErrorHandler('general', Object.assign({}, state), action);
    case MicroserviceUploadAction.onSuccess:
      return onSuccessHandler('general', Object.assign({}, state), action);
    case MicroserviceUploadAction.onReset:
      return defaultGeneralState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __complete: isComplete(stateCopy, generalStepRequiredFields),
    __error: action.payload.error || false
  });
};
const upload = (state = defaultUploadState, action = defaultAction) => {
  let stateCopy;
  let pluginProperties,
      artifactExtends,
      artifactPlugins,
      artifactJson,
      fileMetadataObj;
  switch (action.type) {
    case MicroserviceUploadAction.setFilePath:
      if (!action.payload.file.name.endsWith('.jar')) {
        return Object.assign({}, state, {
          jar: Object.assign({}, defaultJarState, {
            __error: T.translate('features.Wizard.MicroserviceUpload.Step2.errorMessage')
          })
        });
      }
      fileMetadataObj = getArtifactNameAndVersion(action.payload.file.name.split('.jar')[0]);
      stateCopy = Object.assign({}, state, {
        jar: {
          contents: action.payload.file,
          fileMetadataObj
        }
      });
      break;
    case MicroserviceUploadAction.setJson:
      artifactJson = action.payload.json;
      try {
        artifactJson = JSON.parse(artifactJson);
      } catch (e) {
        return Object.assign({}, state, {
          json: Object.assign({}, defaultJsonState, {
            __error: T.translate('features.Wizard.PluginArtifact.Step2.errorMessage')
          })
        });
      }
      if (!artifactJson.parents) {
        return Object.assign({}, state, {
          json: Object.assign({}, defaultJsonState, {
            __error: T.translate('features.Wizard.PluginArtifact.Step2.errorMessageParentArtifacts')
          })
        });
      }
      pluginProperties = artifactJson.properties;
      artifactExtends = artifactJson.parents.reduce( (prev, curr) => `${prev}/${curr}`);
      artifactPlugins = artifactJson.plugins || [];
      stateCopy = Object.assign({}, state, {
        json: {
          properties: pluginProperties,
          artifactExtends,
          artifactPlugins,
          contents: action.payload.jsonFile
        },
        __complete: true
      });
      return stateCopy;
    case MicroserviceUploadAction.onReset:
      return defaultUploadState;
    default:
      return state;
  }

  return Object.assign({}, stateCopy, {
    __error: action.payload.error || false
  });
};
const configure = (state = defaultConfigureState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case MicroserviceUploadAction.setInstances:
      stateCopy = Object.assign({}, state, {
        instances: action.payload.instances
      });
      break;
    case MicroserviceUploadAction.setVCores:
      stateCopy = Object.assign({}, state, {
        vcores: action.payload.vcores
      });
      break;
    case MicroserviceUploadAction.setMemory:
      stateCopy = Object.assign({}, state, {
        memory: action.payload.memory
      });
      break;
    case MicroserviceUploadAction.setThreshold:
      stateCopy = Object.assign({}, state, {
        ethreshold: action.payload.ethreshold
      });
      break;
    case MicroserviceUploadAction.setEndpoints:
      stateCopy = Object.assign({}, state, {
        endpoints: action.payload.endpoints
      });
      break;
    case MicroserviceUploadAction.onError:
      return onErrorHandler('configure', Object.assign({}, state), action);
    case MicroserviceUploadAction.onSuccess:
      return onSuccessHandler('configure', Object.assign({}, state), action);
    case MicroserviceUploadAction.onReset:
      return defaultConfigureState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __complete: isComplete(stateCopy, configureStepRequiredFields),
    __error: action.payload.error || false
  });
};
const properties = (state = defaultPropertiesState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case MicroserviceUploadAction.setProperties:
      stateCopy = Object.assign({}, state, {
        properties: action.payload.properties
      });
      break;
    case MicroserviceUploadAction.onError:
      return onErrorHandler('properties', Object.assign({}, state), action);
    case MicroserviceUploadAction.onSuccess:
      return onSuccessHandler('properties', Object.assign({}, state), action);
    case MicroserviceUploadAction.onReset:
      return defaultPropertiesState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __complete: !isNil(stateCopy.properties),
    __error: action.payload.error || false
  });
};
const endpoints = (state = defaultEndpointsState, action = defaultAction) => {
  let stateCopy;
  switch (action.type) {
    case MicroserviceUploadAction.setEndpoints:
      stateCopy = Object.assign({}, state, {
        endpoints: action.payload.endpoints
      });
      break;
    case MicroserviceUploadAction.onError:
      return onErrorHandler('endpoints', Object.assign({}, state), action);
    case MicroserviceUploadAction.onSuccess:
      return onSuccessHandler('endpoints', Object.assign({}, state), action);
    case MicroserviceUploadAction.onReset:
      return defaultEndpointsState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __complete: !isNil(stateCopy.endpoints),
    __error: action.payload.error || false
  });
};
// Store
const createStoreWrapper = () => {
  return createStore(
    combineReducers({
      general,
      upload,
      configure,
      properties,
      endpoints
    }),
    defaultInitialState
  );
};

const MicroserviceUploadStore = createStoreWrapper();
export default MicroserviceUploadStore;
export {createStoreWrapper};
