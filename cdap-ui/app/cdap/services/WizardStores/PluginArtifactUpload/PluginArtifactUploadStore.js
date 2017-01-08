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
import PluginArtifactUploadActions from 'services/WizardStores/PluginArtifactUpload/PluginArtifactUploadActions';
import {getArtifactNameAndVersion} from 'services/helpers';
import T from 'i18n-react';

const defaultAction = {
  type: '',
  payload: {}
};
const defaultState = {
  __complete: false,
  __skipped: false,
  __error: false
};
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

const defaultInitialState = {
  upload: defaultUploadState
};

const upload = (state = defaultUploadState, action = defaultAction) => {
  let stateCopy;
  let pluginProperties,
      artifactExtends,
      artifactPlugins,
      artifactJson,
      fileMetadataObj;
  switch (action.type) {
    case PluginArtifactUploadActions.setFilePath:
      if (!action.payload.file.name.endsWith('.jar')) {
        return Object.assign({}, state, {
          jar: Object.assign({}, defaultJarState, {
            __error: T.translate('features.Wizard.PluginArtifact.Step1.errorMessage')
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
    case PluginArtifactUploadActions.setJson:
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
        }
      });
      return stateCopy;
    case PluginArtifactUploadActions.onReset:
      return defaultUploadState;
    default:
      return state;
  }

  return Object.assign({}, stateCopy, {
    __complete: true
  });
};


const PluginArtifactUploadStoreWrapper = () => {
  return createStore(
    combineReducers({
      upload
    }),
    defaultInitialState
  );
};

const PluginArtifactUploadStore = PluginArtifactUploadStoreWrapper();
export default PluginArtifactUploadStore;
