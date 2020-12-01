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

import { combineReducers, createStore, Store as InterfaceStore } from 'redux';
import NamespaceActions from './NamespaceActions';
import { composeEnhancers, objectQuery, isNilOrEmpty } from 'services/helpers';
import { SYSTEM_NAMESPACE } from 'services/global-constants';
import { MyNamespaceApi } from 'api/namespace';
import { IAction } from 'services/redux-helpers';
import find from 'lodash/find';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import { GLOBALS } from 'services/global-constants';

export interface INamespace {
  name: string;
  description: string;
  config: any;
}

export interface INamespaceStoreState {
  username: string;
  selectedNamespace: string;
  namespaces: INamespace[];
}
const defaultAction = {
  type: '',
  payload: {} as any,
};

const defaultInitialState = {
  username: '',
  selectedNamespace: '',
  namespaces: [],
};

const username = (state = '', action: IAction = defaultAction) => {
  switch (action.type) {
    case NamespaceActions.updateUsername:
      return action.payload.username;
    default:
      return state;
  }
};

const selectedNamespace = (state = '', action: IAction = defaultAction) => {
  switch (action.type) {
    case NamespaceActions.selectNamespace: {
      if (action.payload.selectedNamespace === SYSTEM_NAMESPACE) {
        return localStorage.getItem('CurrentNamespace');
      }
      return action.payload.selectedNamespace;
    }
    case NamespaceActions.updateNamespaces: {
      const previouslyAccessedNs = localStorage.getItem('CurrentNamespace');
      if (isNilOrEmpty(state) || state === SYSTEM_NAMESPACE) {
        return !isNilOrEmpty(previouslyAccessedNs)
          ? previouslyAccessedNs
          : objectQuery(action.payload, 'namespaces', 0, 'name');
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
/**
 * Store to manage namespace globally across CDAP UI.
 * Stores list of namespaces and current namespace the user is in.
 */
const NamespaceStore: InterfaceStore<INamespaceStoreState> = createStore(
  combineReducers({
    username,
    selectedNamespace,
    namespaces,
  }),
  defaultInitialState,
  composeEnhancers('NamespaceStore')()
);

const getCurrentNamespace = () => {
  const { selectedNamespace: namespace } = NamespaceStore.getState();
  return namespace;
};

const fetchNamespaceList = async () => {
  const eventEmitter = ee(ee);
  try {
    const namespaceList = await MyNamespaceApi.list().toPromise();
    if (Array.isArray(namespaceList) && namespaceList.length === 0) {
      eventEmitter.emit(globalEvents.NONAMESPACE);
      return Promise.reject();
    }
  } catch (e) {
    // tslint:disable-next-line: no-console
    console.log('Fetching namespace list fails: ', e);
    return Promise.reject();
  }
  return;
};

const fetchNamespaceDetails = async (namespace: string) => {
  const eventEmitter = ee(ee);
  try {
    await MyNamespaceApi.get({ namespace }).toPromise();
  } catch (err) {
    if (err.statusCode > 400) {
      let message;
      switch (err.statusCode) {
        case 403:
          message = GLOBALS.pageLevelErrors['UNAUTHORIZED-NAMESPACE'](namespace);
          break;
        default:
          message = GLOBALS.pageLevelErrors['UNKNOWN-NAMESPACE'](namespace);
      }
      eventEmitter.emit(globalEvents.PAGE_LEVEL_ERROR, {
        statusCode: err.statusCode,
        data: message,
      });
      return Promise.reject(false);
    }
  }
};

const validateNamespace = async (namespace) => {
  if (!namespace) {
    return;
  }
  if (namespace === SYSTEM_NAMESPACE) {
    return Promise.resolve(true);
  }
  await fetchNamespaceList();
  return await fetchNamespaceDetails(namespace);
};

const getValidNamespace = async (currentNs) => {
  const list = await MyNamespaceApi.list().toPromise();
  if (!list || list.length === 0) {
    return null;
  }
  const validNs = list.find((ns: INamespace) => ns.name === currentNs);
  if (!validNs) {
    const findNamespace = (nsList, name) => {
      return find(nsList, { name });
    };

    /*
     * 1. Check if localStorage has a 'DefaultNamespace' set by the user, if not,
     * 2. Check if there is a 'default' namespace from backend, if not,
     * 3. Take first one from the list of namespaces from backend.
     */

    let selectedNs;
    let defaultNamespace;

    // Check #1
    if (!selectedNs) {
      defaultNamespace = localStorage.getItem('DefaultNamespace');
      const defaultNsFromBackend = list.filter((ns) => ns.name === defaultNamespace);
      if (defaultNsFromBackend.length) {
        selectedNs = defaultNsFromBackend[0];
      }
    }
    // Check #2
    if (!selectedNs) {
      selectedNs = findNamespace(list, 'default');
    }
    // Check #3
    if (!selectedNs) {
      selectedNs = list[0].name;
    } else {
      selectedNs = selectedNs.name;
    }

    return selectedNs;
  }
  return validNs.name;
};

export default NamespaceStore;
export {
  getCurrentNamespace,
  validateNamespace,
  getValidNamespace,
  fetchNamespaceDetails,
  fetchNamespaceList,
};
