/*
 * Copyright © 2018 Cask Data, Inc.
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

import { createStore } from 'redux';
import { defaultAction, composeEnhancers } from 'services/helpers';

const NamespaceDetailsActions = {
  enableLoading: 'NAMESPACE_DETAILS_ENABLE_LOADING',
  setData: 'NAMESPACE_DETAILS_SET_DATA',
  reset: 'NAMESPACE_DETAILS_RESET',
};

const defaultInitialState = {
  name: '',
  description: '',
  customAppCount: 0,
  pipelineCount: 0,
  datasetCount: 0,
  namespacePrefs: {},
  systemPrefs: {},
  hdfsRootDirectory: '',
  hbaseNamespaceName: '',
  hiveDatabaseName: '',
  schedulerQueueName: '',
  principal: '',
  keytabURI: '',
  loading: false,
};

const namespaceDetails = (state = defaultInitialState, action = defaultAction) => {
  switch (action.type) {
    case NamespaceDetailsActions.setData:
      return {
        ...state,
        ...action.payload,
        loading: false,
      };
    case NamespaceDetailsActions.enableLoading:
      return {
        ...state,
        loading: true,
      };
    case NamespaceDetailsActions.reset:
      return defaultInitialState;
    default:
      return state;
  }
};

const NamespaceDetails = createStore(
  namespaceDetails,
  defaultInitialState,
  composeEnhancers('NamespaceDetailsStore')()
);

export default NamespaceDetails;
export { NamespaceDetailsActions };
