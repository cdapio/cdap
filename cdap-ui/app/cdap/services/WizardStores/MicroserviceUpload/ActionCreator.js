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
import Rx from 'rx';
import NamespaceStore from 'services/NamespaceStore';
import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import {findHighestVersion} from 'services/VersionRange/VersionUtilities';
import {objectQuery} from 'services/helpers';
import {MyArtifactApi} from 'api/artifact';
import {MyPipelineApi} from 'api/pipeline';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';

const uploadArtifact = () => {
  const state = MicroserviceUploadStore.getState();
  if (!state.general.showNewMicroserviceTextbox) {
    return Rx.Observable.of([]);
  }

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
  if (!state.general.showNewMicroserviceTextbox) {
    return Rx.Observable.of([]);
  }

  let {name: artifactId, version} = state.uploadjar.fileMetadataObj;
  let namespace = NamespaceStore.getState().selectedNamespace;
  let artifactConfigurationProperties = state.uploadjson.properties;
  return MyArtifactApi
    .loadPluginConfiguration({
      namespace,
      artifactId,
      version
    }, artifactConfigurationProperties);
};

const findMicroserviceArtifact = () => {
  let namespace = NamespaceStore.getState().selectedNamespace;

  return MyArtifactApi
    .list({ namespace })
    .flatMap((artifacts) => {
      let microserviceArtifacts = artifacts.filter((artifact) => {
        return artifact.name === 'microservice-app';
      });

      if (microserviceArtifacts.length === 0) {
        return Rx.Observable.of({});
      }

      let highestVersion = findHighestVersion(microserviceArtifacts.map((artifact) => {
        return artifact.version;
      }), true);

      let filteredArtifacts = microserviceArtifacts;

      filteredArtifacts = microserviceArtifacts.filter((artifact) => {
        return artifact.version === highestVersion;
      });

      let returnArtifact = filteredArtifacts[0];

      if (filteredArtifacts.length > 1) {
        returnArtifact.scope = 'USER';
      }

      return Rx.Observable.of(returnArtifact);
    });
};

const listMicroservicePlugins = (artifact) => {
  if (isEmpty(artifact)) {
    return Rx.Observable.of([]);
  }
  let namespace = NamespaceStore.getState().selectedNamespace;
  let {name: artifactId, version, scope} = artifact;

  let pluginParams = {
    namespace,
    artifactId,
    version,
    scope
  };

  return MyArtifactApi.listMicroservicePlugins(pluginParams);
};

const createApplication = () => {
  const state = MicroserviceUploadStore.getState();
  let namespace = NamespaceStore.getState().selectedNamespace;
  let { instanceName: appId, description: appDescription, version: appVersion } = state.general;
  let pluginId, artifactId, artifactVersion, artifactScope;

  if (state.general.showNewMicroserviceTextbox) {
    pluginId = state.general.newMicroserviceName;
    artifactId = objectQuery(state, 'uploadjar', 'fileMetadataObj', 'name');
    artifactVersion = objectQuery(state, 'uploadjar', 'fileMetadataObj', 'version');
    artifactScope = 'USER';
  } else {
    pluginId = state.general.microserviceOption;
    let pluginArtifact = state.general.defaultMicroservicePlugins.filter(plugin => plugin.name === pluginId);
    if (pluginArtifact.length > 0) {
      pluginArtifact = pluginArtifact[0];
      artifactId = objectQuery(pluginArtifact, 'artifact', 'name');
      artifactVersion = objectQuery(pluginArtifact, 'artifact', 'version');
      artifactScope = objectQuery(pluginArtifact, 'artifact', 'scope');
    }
  }

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
        scope: artifactScope
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

  return findMicroserviceArtifact()
    .flatMap((artifact) => {
      return MyPipelineApi
        .publish({
          namespace,
          appId
          }, {
            artifact,
            config
          }
        );
    });
};

const MicroserviceUploadActionCreator = {
  uploadArtifact,
  uploadConfigurationJson,
  createApplication,
  findMicroserviceArtifact,
  listMicroservicePlugins
};

export default MicroserviceUploadActionCreator;
