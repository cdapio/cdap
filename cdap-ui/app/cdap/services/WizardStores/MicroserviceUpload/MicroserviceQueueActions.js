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

const MicroserviceQueueActions = {
  addQueue: 'ADD-MQ',
  deleteQueue: 'DELETE-MQ',
  setName: 'SET-MQ-NAME',
  setType: 'SET-MQ-TYPE',
  setNamespace: 'SET-MQ-NAMESPACE',
  setTopic: 'SET-MQ-TOPIC',
  setRegion: 'SET-MQ-SQS-REGION',
  setAccessKey: 'SET-MQ-ACCESS-KEY',
  setAccessId: 'SET-MQ-ACCESS-ID',
  setQueueName: 'SET-MQ-QUEUE-NAME',
  setEndpoint: 'SET-MQ-ENDPOINT',
  setConnection: 'SET-MQ-CONNECTION',
  setSSLKeystoreFilePath: 'SET-MQ-SSL-KEYSTORE-FILE-PATH',
  setSSLKeystorePassword: 'SET-MQ-SSL-KEYSTORE-PASSWORD',
  setSSLKeystoreKeyPassword: 'SET-MQ-SSL-KEYSTORE-KEY-PASSWORD',
  setSSLKeystoreType: 'SET-MQ-SSL-KEYSTORE-TYPE',
  setSSLTruststoreFilePath: 'SET-MQ-SSL-TRUSTSTORE-FILE-PATH',
  setSSLTruststorePassword: 'SET-MQ-SSL-TRUSTSTORE-PASSWORD',
  setSSLTruststoreType: 'SET-MQ-SSL-TRUSTSTORE-TYPE',
  setMapRTopic: 'SET-MQ-MAPR-TOPIC',
  setKeySerdes: 'SET-MQ-KEY-SERDES',
  setValueSerdes: 'SET-MQ-VALUE-SERDES',
  onReset: 'SET-MQ-RESET',
  onUpdate: 'SET-MQ-UPDATE'
};

export default MicroserviceQueueActions;
