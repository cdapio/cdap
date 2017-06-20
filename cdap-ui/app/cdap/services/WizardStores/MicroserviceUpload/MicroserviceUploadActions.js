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

const MicroserviceUploadAction = {
  setInstanceName: 'SET-MICROSERVICE-INSTANCE-NAME',
  setDescription: 'SET-MICROSERVICE-DESCRIPTION',
  setVersion: 'SET-MICROSERVICE-VERSION',
  setMicroserviceName: 'SET-MICROSERVICE-NAME',
  setInstances: 'SET-MICROSERVICE-INSTANCES',
  setVCores: 'SET-MICROSERVICE-VCORES',
  setMemory: 'SET-MICROSERVICE-MEMORY',
  setThreshold: 'SET-MICROSERVICE-ETHRESHOLD',
  setEndpoints: 'SET-MICROSERVICE-ENDPOINTS',
  setInboundQueues: 'SET-MICROSERVICE-INBOUND-QUEUES',
  setOutboundQueues: 'SET-MICROSERVICE-OUTBOUND-QUEUES',
  setFetchSize: 'SET-MICROSERVICE-FETCH-SIZE',
  setProperties: 'SET-MICROSERVICE-PROPERTIES',
  setFilePath: 'SET-MICROSERVICE-JAR',
  setJson: 'SET-MICROSERVICE-JSON',

  onError: 'FORM-SUBMIT-FAILURE',
  onSuccess: 'FORM-SUBMIT-SUCCESS',
  onReset: 'FORM-RESET'
};
export default MicroserviceUploadAction;
