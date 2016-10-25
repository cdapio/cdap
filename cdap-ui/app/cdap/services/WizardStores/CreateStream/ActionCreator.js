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
import CreateStreamStore from 'services/WizardStores/CreateStream/CreateStreamStore';
import {MyStreamApi} from 'api/stream';
import NamespaceStore from 'services/NamespaceStore';

const PublishStream = () => {
  let state = CreateStreamStore.getState();
  let urlParams = {
    namespace: NamespaceStore.getState().selectedNamespace,
    streamId: state.general.name
  };
  let putParams = {};
  if (state.general.ttl) {
    putParams.ttl = state.general.ttl;
  }
  if (state.schema.format) {
    putParams.format = putParams.format || {};
    putParams.format.name = state.schema.format;
  }
  if (state.schema.value) {
    putParams.format = putParams.format || {};
    putParams.format.schema = state.schema.value;
  }
  if (state.threshold.value) {
    putParams['notification.threshold.mb'] = state.threshold.value;
  }
  if (state.general.description) {
    putParams.description = state.general.description;
  }

  return MyStreamApi
    .create(urlParams, putParams);
};

export {PublishStream};
