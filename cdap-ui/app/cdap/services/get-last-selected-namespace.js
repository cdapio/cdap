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

import find from 'lodash/find';
import NamespaceStore from 'services/NamespaceStore';

function findNamespace(list, name) {
  return find(list, {name: name});
}

/**
 * 1. Check if localStorage has a 'DefaultNamespace' set by the user, if not,
 * 2. Check if there is a 'default' namespace from backend, if not,
 * 3. Take first one from the list of namespaces from backend.
 **/
function getDefaultNamespace() {
  let list = NamespaceStore.getState().namespaces;
  if (!list || list.length === 0) { return; }
  let selectedNamespace;
  let defaultNamespace = localStorage.getItem('DefaultNamespace');
  let defaultNsFromBackend = list.filter(ns => ns.name === defaultNamespace);
  if (defaultNsFromBackend.length) {
    selectedNamespace = defaultNsFromBackend[0];
  }
  // Check #2
  if (!selectedNamespace) {
    selectedNamespace = findNamespace(list, 'default');
  }
  // Check #3
  if (!selectedNamespace) {
    selectedNamespace = list[0].name;
  } else {
    selectedNamespace = selectedNamespace.name;
  }
  return selectedNamespace;
}

/**
 * 1. Check if the Namespace store state contains the last selected namespace, if not,
 * 2. Check if local storage has that info, if not,
 * 3. Get the 'default' namespace.
 **/
export default function getLastSelectedNamespace() {
  return NamespaceStore.getState().selectedNamespace || localStorage.getItem('CurrentNamespace') || getDefaultNamespace();
}
