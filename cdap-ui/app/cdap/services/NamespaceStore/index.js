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

import {combineReducers, createStore} from 'redux';
import NamespaceActions from './NamespaceActions';
import {composeEnhancers, objectQuery, isNilOrEmpty} from 'services/helpers';
import {SYSTEM_NAMESPACE} from 'components/Administration';

const defaultAction = {
  action : '',
  payload : {}
};

const defaultInitialState = {
  username : '',
  selectedNamespace : '',
  namespaces : []
};

const username = (state = '', action = defaultAction) => {
  switch (action.type) {
    case NamespaceActions.updateUsername:
        return action.payload.username;
    default:
      return state;
  }
};

const selectedNamespace = (state = '', action = defaultAction) => {
  switch (action.type) {
    case NamespaceActions.selectNamespace: {
      if (action.payload.selectedNamespace === SYSTEM_NAMESPACE) {
        return localStorage.getItem('CurrentNamespace');
      }
      return action.payload.selectedNamespace;
    }
    case NamespaceActions.updateNamespaces: {
      let previouslyAccessedNs = localStorage.getItem('CurrentNamespace');
      if (isNilOrEmpty(state) || state === SYSTEM_NAMESPACE) {
        return !isNilOrEmpty(previouslyAccessedNs) ? previouslyAccessedNs : objectQuery(action.payload, 'namespaces', 0, 'name');
      }
      return state;
    }
    default:
      return state;
  }
};

const namespaces = (state = [], action) => {
    switch (action.type) {
      case NamespaceActions.updateNamespaces:
        return action.payload.namespaces;
      default:
        return state;
    }
};

const NamespaceStore = createStore(
  combineReducers({
    username,
    selectedNamespace,
    namespaces
  }),
  defaultInitialState,
  composeEnhancers('NamespaceStore')()
);

const getCurrentNamespace = () => {
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  return namespace;
};

export default NamespaceStore;
export {getCurrentNamespace};
