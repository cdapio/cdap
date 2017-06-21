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

import UploadFile from 'services/upload-file';
import cookie from 'react-cookie';
import NamespaceStore from 'services/NamespaceStore';
import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import {MyArtifactApi} from 'api/artifact';
import {MyPipelineApi} from 'api/pipeline';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';

const uploadArtifact = () => {
  const state = MicroserviceUploadStore.getState();
  let {name, version} = state.uploadjar.fileMetadataObj;
  let namespace = NamespaceStore.getState().selectedNamespace;
  let url = `/namespaces/${namespace}/artifacts/${name}`;
  let headers = {
    'Content-Type': 'application/octet-stream',
    'Artifact-Version': version,
    'Artifact-Extends': state.uploadjson.artifactExtends
  };
  if (window.CDAP_CONFIG.securityEnabled) {
    let token = cookie.load('CDAP_Auth_Token');
    if (!isNil(token)) {
      headers.Authorization = `Bearer ${token}`;
    }
  }
  return UploadFile({url, fileContents: state.uploadjar.contents, headers});
};

const uploadConfigurationJson = () => {
  const state = MicroserviceUploadStore.getState();
  let {name:artifactId, version} = state.uploadjar.fileMetadataObj;
  let namespace = NamespaceStore.getState().selectedNamespace;
  let artifactConfigurationProperties = state.uploadjson.properties;
  return MyArtifactApi
    .loadPluginConfiguration({
      namespace,
      artifactId,
      version
    }, artifactConfigurationProperties);
};

const createApplication = () => {
  const state = MicroserviceUploadStore.getState();
  let namespace = NamespaceStore.getState().selectedNamespace;
  let { instanceName: appId, description: appDescription, version: appVersion, microserviceName: pluginId } = state.general;
  let { name: artifactId, version: artifactVersion } = state.uploadjar.fileMetadataObj;
  let { instances, vcores, memory, ethreshold } = state.configure;

  let config = {
    version: appVersion,
    id: appId,
    description: appDescription,
    plugin: {
      name: pluginId,
      artifact: {
        name: artifactId,
        version: artifactVersion,
        scope: 'user'
      }
    },
    configuration: {
      instances,
      vcores,
      memory,
      ethreshold
    }
  };

  let endpoints = state.endpoints;
  let endpointsObj = {
    fetch: '',
    in: [],
    out: []
  };

  if (typeof(endpoints.fetch) !== 'number') {
    delete endpointsObj.fetch;
  } else {
    endpointsObj.fetch = endpoints.fetch;
  }

  endpoints.in.forEach((inboundQueue) => {
    if (inboundQueue.property.length > 0) {
      endpointsObj.in.push(inboundQueue.property);
    }
  });
  if (endpointsObj.in.length === 0) {
    delete endpointsObj.in;
  }

  endpoints.out.forEach((outboundQueue) => {
    if (outboundQueue.property.length > 0) {
      endpointsObj.out.push(outboundQueue.property);
    }
  });
  if (endpointsObj.out.length === 0) {
    delete endpointsObj.out;
  }

  if (!isEmpty(endpointsObj)) {
    config.configuration.endpoints = endpointsObj;
  }

  let properties = state.properties;
  let propertiesKeyVal = properties.keyValues.pairs;
  let propertiesObj = {};
  propertiesKeyVal.forEach((pair) => {
    if (pair.key.length > 0 && pair.value.length > 0) {
      propertiesObj[pair.key] = pair.value;
    }
  });
  if (!isEmpty(propertiesObj)) {
    config.configuration.properties = propertiesObj;
  }

  let artifact = {
    name: 'microservice-app',
    version: '1.0-SNAPSHOT',
    scope: 'user'
  };

  return MyPipelineApi
    .publish({
      namespace,
      appId
      }, {
        artifact,
        config
      }
    );
};

const MicroserviceUploadActionCreator = {
  uploadArtifact,
  uploadConfigurationJson,
  createApplication
};

export default MicroserviceUploadActionCreator;
