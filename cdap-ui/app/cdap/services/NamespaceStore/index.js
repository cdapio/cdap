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
    case NamespaceActions.selectNamespace:
      return action.payload.selectedNamespace;
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
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

const getCurrentNamespace = () => {
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  return namespace;
};

export default NamespaceStore;
export {getCurrentNamespace};
