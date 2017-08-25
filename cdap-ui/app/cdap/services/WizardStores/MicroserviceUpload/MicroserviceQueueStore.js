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
import MicroserviceQueueActions from 'services/WizardStores/MicroserviceUpload/MicroserviceQueueActions';
import NamespaceStore from 'services/NamespaceStore';
import {defaultAction} from 'services/helpers';
import cloneDeep from 'lodash/cloneDeep';
import isNil from 'lodash/isNil';
import T from 'i18n-react';

const PREFIX = 'features.Wizard.MicroserviceUpload.MicroserviceQueue';

const defaultQueueTypes = [
  {
    id: 'tms',
    value: T.translate(`${PREFIX}.types.tms`),
    properties: ['namespace', 'topic']
  },
  {
    id: 'sqs',
    value: T.translate(`${PREFIX}.types.sqs`),
    properties: ['region', 'access-key', 'access-id', 'queue-name', 'endpoint']
  },
  {
    id: 'websocket',
    value: T.translate(`${PREFIX}.types.websocket`),
    properties: ['connection', 'ssl-keystore-file-path', 'ssl-keystore-password', 'ssl-keystore-key-password', 'ssl-keystore-type', 'ssl-truststore-file-path', 'ssl-truststore-password', 'ssl-truststore-type']
  },
  {
    id: 'mapr-stream',
    value: T.translate(`${PREFIX}.types.mapr-stream`),
    properties: ['mapRTopic', 'key-serdes', 'value-serdes']
  }
];

const defaultGeneralState = {
  queues: []
};

// Reducers
const general = (state = defaultGeneralState, action = defaultAction) => {
  let stateCopy = {};

  switch (action.type) {
    case MicroserviceQueueActions.addQueue:
      stateCopy = cloneDeep(state);
      stateCopy.queues.splice(action.payload.index + 1, 0, {
        name: '',
        type: defaultQueueTypes[0].id,
        properties: {
          namespace: NamespaceStore.getState().selectedNamespace,
          topic: ''
        }
      });
      break;
    case MicroserviceQueueActions.deleteQueue:
      stateCopy = cloneDeep(state);
      if (!stateCopy.queues || !stateCopy.queues.length) { return stateCopy; }
      stateCopy.queues.splice(action.payload.index, 1);
      break;
    case MicroserviceQueueActions.setName:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.name)) { return stateCopy; }
      stateCopy.queues[action.payload.index].name = action.payload.name;
      break;
    case MicroserviceQueueActions.setType:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.type)) { return stateCopy; }
      stateCopy.queues[action.payload.index].type = action.payload.type;
      break;
    case MicroserviceQueueActions.setNamespace:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.namespace)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties.namespace = action.payload.namespace;
      break;
    case MicroserviceQueueActions.setTopic:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.topic)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties.topic = action.payload.topic;
      break;
    case MicroserviceQueueActions.setRegion:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.region)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties.region = action.payload.region;
      break;
    case MicroserviceQueueActions.setAccessKey:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.accessKey)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties['access-key'] = action.payload.accessKey;
      break;
    case MicroserviceQueueActions.setAccessId:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.accessId)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties['access-id'] = action.payload.accessId;
      break;
    case MicroserviceQueueActions.setQueueName:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.queueName)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties['queue-name'] = action.payload.queueName;
      break;
    case MicroserviceQueueActions.setEndpoint:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.endpoint)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties.endpoint = action.payload.endpoint;
      break;
    case MicroserviceQueueActions.setConnection:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.connection)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties.connection = action.payload.connection;
      break;
    case MicroserviceQueueActions.setSSLKeystoreFilePath:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.sslKeystoreFilePath)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties['ssl-keystore-file-path'] = action.payload.sslKeystoreFilePath;
      break;
    case MicroserviceQueueActions.setSSLKeystorePassword:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.sslKeystorePassword)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties['ssl-keystore-password'] = action.payload.sslKeystorePassword;
      break;
    case MicroserviceQueueActions.setSSLKeystoreKeyPassword:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.sslKeystoreKeyPassword)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties['ssl-keystore-key-password'] = action.payload.sslKeystoreKeyPassword;
      break;
    case MicroserviceQueueActions.setSSLKeystoreType:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.sslKeystoreType)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties['ssl-keystore-type'] = action.payload.sslKeystoreType;
      break;
    case MicroserviceQueueActions.setSSLTruststoreFilePath:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.sslTruststoreFilePath)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties['ssl-truststore-file-path'] = action.payload.sslTruststoreFilePath;
      break;
    case MicroserviceQueueActions.setSSLTruststorePassword:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.sslTruststorePassword)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties['ssl-truststore-password'] = action.payload.sslTruststorePassword;
      break;
    case MicroserviceQueueActions.setSSLTruststoreType:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.sslTruststoreType)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties['ssl-truststore-type'] = action.payload.sslTruststoreType;
      break;
    case MicroserviceQueueActions.setMapRTopic:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.mapRTopic)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties.mapRTopic = action.payload.mapRTopic;
      break;
    case MicroserviceQueueActions.setKeySerdes:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.keySerdes)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties['key-serdes'] = action.payload.keySerdes;
      break;
    case MicroserviceQueueActions.setValueSerdes:
      stateCopy = cloneDeep(state);
      if (isNil(action.payload.valueSerdes)) { return stateCopy; }
      stateCopy.queues[action.payload.index].properties['value-serdes'] = action.payload.valueSerdes;
      break;

    case MicroserviceQueueActions.onUpdate:
      stateCopy = cloneDeep(state);
      stateCopy.queues = action.payload.queues;
      break;
    case MicroserviceQueueActions.onReset:
      return defaultGeneralState;
    default:
      return state;
  }
  return stateCopy;
};

// Store
const createMicroserviceQueueStore = (initialState = defaultGeneralState) => {
  return createStore(
    combineReducers({general}),
    initialState
  );
};

const MicroserviceQueueStore = createMicroserviceQueueStore();
export default MicroserviceQueueStore;
export {createMicroserviceQueueStore, defaultQueueTypes};
