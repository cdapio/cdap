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

import React, {Component} from 'react';
import find from 'lodash/find';
import NamespaceStore from 'services/NamespaceStore';
import Redirect from 'react-router/Redirect';

export default class RouteToNamespace extends Component {
  constructor(props) {
    super(props);
    this.state = {
      selectedNamespace: null
    };
  }

  componentWillMount() {
    this.setNamespace();
    this.sub = NamespaceStore.subscribe(this.setNamespace.bind(this));
  }

  componentWillUnmount() {
    this.sub();
  }

  findNamespace(list, name) {
    return find(list, {name: name});
  }

  setNamespace() {
    let list = NamespaceStore.getState().namespaces;

    if (!list || list.length === 0) { return; }

    /**
     * 1. Check if localStorage has a 'DefaultNamespace' set by the user, if not,
     * 2. Check if there is a 'default' namespace from backend, if not,
     * 3. Take first one from the list of namespaces from backend.
     **/

    let selectedNamespace;
    let defaultNamespace;

    //Check #1
    if(!selectedNamespace){
      defaultNamespace = localStorage.getItem('DefaultNamespace');
      let defaultNsFromBackend = list.filter(ns => ns.name === defaultNamespace);
      if (defaultNsFromBackend.length) {
        selectedNamespace = defaultNsFromBackend[0];
      }
    }
    //Check #2
    if(!selectedNamespace) {
      selectedNamespace = this.findNamespace(list, 'default');
    }
    //Check #3
    if(!selectedNamespace){
      selectedNamespace = list[0].name;
    } else {
      selectedNamespace = selectedNamespace.name;
    }

    localStorage.setItem('DefaultNamespace', selectedNamespace);
    this.setState({selectedNamespace});
  }

  render() {
    if (!this.state.selectedNamespace) {
      return null;
    }
    return <Redirect to={`/ns/${this.state.selectedNamespace}`} />;
  }
}
