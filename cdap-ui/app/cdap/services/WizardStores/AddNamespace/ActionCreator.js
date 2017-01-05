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
import AddNamespaceStore from 'services/WizardStores/AddNamespace/AddNamespaceStore';
import {MyNamespaceApi} from 'api/namespace';

const PublishNamespace = () => {
  let state = AddNamespaceStore.getState();
  let urlParams = {
    namespace: state.general.name
  };

  let putParams = {
    config : {}
  };

  if (state.general.description) {
    putParams["description"] = state.general.description;
  }

  if(state.mapping.hbaseNamespace){
    putParams["config"]["hbase.namespace"] = state.mapping.hbaseNamespace;
  }

  if(state.mapping.hiveDatabaseName){
    putParams["config"]["hive.database"] = state.mapping.hiveDatabaseName;
  }

  if(state.mapping.hdfsDirectory){
    putParams["config"]["root.directory"] = state.mapping.hdfsDirectory;
  }

  if(state.security.keyTab){
    putParams["config"]["keytabURI"] = state.security.keyTab;
  }

  if(state.security.principal){
    putParams["config"]["principal"] = state.security.principal;
  }

  if(state.preferences.schedulerQueueName){
    putParams["config"]["scheduluer.queue.name"] = state.preferences.schedulerQueueName;
  }

  return MyNamespaceApi
    .create(urlParams, putParams);
};

const PublishPreferences = () => {
  let state = AddNamespaceStore.getState();
  let urlParams = {
    namespace: state.general.name
  };
  let preferences = {};

  if(state.preferences.keyValues && state.preferences.keyValues.pairs.length > 0){
    state.preferences.keyValues.pairs.forEach((pair) => {
      preferences[pair.key] = pair.value;
    });

    return MyNamespaceApi
      .setPreferences(urlParams, preferences);
  }
};

export {PublishNamespace, PublishPreferences};
