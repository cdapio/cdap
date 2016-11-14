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

import UploadFile from 'services/upload-file';
import cookie from 'react-cookie';
import NamespaceStore from 'services/NamespaceStore';
import PluginArtifactUploadStore from 'services/WizardStores/PluginArtifactUpload/PluginArtifactUploadStore';
import {MyArtifactApi} from 'api/artifact';

const uploadArtifact = () => {
  const state = PluginArtifactUploadStore.getState();
  let {name, version} = state.upload.jar.fileMetadataObj;
  let namespace = NamespaceStore.getState().selectedNamespace;
  let url = `/namespaces/${namespace}/artifacts/${name}`;
  let headers = {
    'Content-Type': 'application/octet-stream',
    'Artifact-Version': version,
    'Artifact-Extends': state.upload.json.artifactExtends,
    'Artifact-Plugins': state.upload.json.artifactPlugins
  };
  if (window.CDAP_CONFIG.securityEnabled) {
    let token = cookie.load('CDAP_Auth_Token');
    headers.Authorization = `Bearer ${token}`;
  }
  return UploadFile({url, fileContents: state.upload.jar.contents, headers});
};

const uploadConfigurationJson = () => {
  const state = PluginArtifactUploadStore.getState();
  let {name:artifactId, version} = state.upload.jar.fileMetadataObj;
  let namespace = NamespaceStore.getState().selectedNamespace;
  let artifactConfigurationProperties = state.upload.json.properties;
  return MyArtifactApi
    .loadPluginConfiguration({
      namespace,
      artifactId,
      version
    }, artifactConfigurationProperties);
};

const PluginArtifactUploadActionCreator = {
  uploadArtifact,
  uploadConfigurationJson
};

export default PluginArtifactUploadActionCreator;
