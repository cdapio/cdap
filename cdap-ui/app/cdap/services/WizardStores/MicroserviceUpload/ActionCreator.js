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
import {Observable} from 'rxjs/Observable';
import NamespaceStore from 'services/NamespaceStore';
import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import {defaultQueueTypes} from 'services/WizardStores/MicroserviceUpload/MicroserviceQueueStore';
import {findHighestVersion} from 'services/VersionRange/VersionUtilities';
import {objectQuery} from 'services/helpers';
import {MyArtifactApi} from 'api/artifact';
import {MyPipelineApi} from 'api/pipeline';
import isNil from 'lodash/isNil';
import isEmpty from 'lodash/isEmpty';

const uploadArtifact = () => {
  const state = MicroserviceUploadStore.getState();
  if (!state.general.showNewMicroserviceTextbox) {
    return Observable.of([]);
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
    return Observable.of([]);
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
    .mergeMap((artifacts) => {
      let microserviceArtifacts = artifacts.filter((artifact) => {
        return artifact.name === 'microservice-core';
      });

      if (microserviceArtifacts.length === 0) {
        return Observable.of({});
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

      return Observable.of(returnArtifact);
    });
};

const listMicroservicePlugins = (artifact) => {
  if (isEmpty(artifact)) {
    return Observable.of([]);
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

const getMicroservicePluginProperties = (pluginId) => {
  let namespace = NamespaceStore.getState().selectedNamespace;
  let { microserviceArtifact } = MicroserviceUploadStore.getState().general;
  let {name: artifactId, version, scope} = microserviceArtifact;

  let pluginParams = {
    namespace,
    artifactId,
    version,
    pluginId,
    scope
  };

  return MyArtifactApi.gettMicroservicePluginDetails(pluginParams);
};

const getTrimmedMicroserviceQueueObj = (queueObj) => {
  let trimmedObj = {};
  trimmedObj.name = queueObj.name;
  trimmedObj.type = queueObj.type;
  trimmedObj.properties = {};

  let defaultTypeProperties = [];
  let defaultTypeObj = defaultQueueTypes.filter(type => type.id === queueObj.type);
  if (defaultTypeObj.length > 0 && defaultTypeObj[0].properties) {
    defaultTypeProperties = defaultTypeObj[0].properties;
  }

  Object.keys(queueObj.properties).forEach(queueProperty => {
    if (defaultTypeProperties.indexOf(queueProperty) !== -1) {

      // Need to add special case for this, because the backend expects the property 'topic'
      // for queues of type 'mapr-stream', but we already have the same property for queues
      // of type TMS. If we use the same name then it would show up in both places
      if (queueProperty === 'mapRTopic') {
        trimmedObj.properties['topic'] = queueObj.properties[queueProperty];
      } else {
        trimmedObj.properties[queueProperty] = queueObj.properties[queueProperty];
      }
    }
  });

  return trimmedObj;
};

const createApplication = () => {
  const state = MicroserviceUploadStore.getState();
  let namespace = NamespaceStore.getState().selectedNamespace;
  let { instanceName: appId, description: appDescription, version: appVersion, microserviceArtifact } = state.general;
  appVersion = parseInt(appVersion, 10) || appVersion;
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

  instances = parseInt(instances, 10) || instances;
  vcores = parseInt(vcores, 10) || vcores;
  memory = parseInt(memory, 10) || memory;
  ethreshold = parseInt(ethreshold, 10) || ethreshold;

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

  let inboundQueues = state.inboundQueues;
  let outboundQueues = state.outboundQueues;

  let endpointsObj = {
    fetch: '',
    in: [],
    out: []
  };

  let fetchSize = inboundQueues.fetch;

  if (fetchSize === '') {
    delete endpointsObj.fetch;
  } else {
    fetchSize = parseInt(fetchSize, 10) || fetchSize;
    endpointsObj.fetch = fetchSize;
  }

  inboundQueues.queues.forEach((inboundQueue) => {
    let trimmedQueueObj = getTrimmedMicroserviceQueueObj(inboundQueue);
    endpointsObj.in.push(trimmedQueueObj);
  });

  if (endpointsObj.in.length === 0) {
    delete endpointsObj.in;
  }

  outboundQueues.queues.forEach((outboundQueue) => {
    let trimmedQueueObj = getTrimmedMicroserviceQueueObj(outboundQueue);
    endpointsObj.out.push(trimmedQueueObj);
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

  return MyPipelineApi
    .publish({
      namespace,
      appId
      }, {
        artifact: microserviceArtifact,
        config
      }
    );
};

const MicroserviceUploadActionCreator = {
  uploadArtifact,
  uploadConfigurationJson,
  createApplication,
  findMicroserviceArtifact,
  listMicroservicePlugins,
  getMicroservicePluginProperties
};

export default MicroserviceUploadActionCreator;
