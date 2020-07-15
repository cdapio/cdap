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

import React, { Component } from 'react';
import find from 'lodash/find';
import NamespaceStore, { isValidNamespace, getValidNamespace } from 'services/NamespaceStore';
import { Redirect } from 'react-router-dom';
import globalEvents from 'services/global-events';
import ee from 'event-emitter';
import { GLOBALS } from 'services/global-constants';

export default class RouteToNamespace extends Component {
  constructor(props) {
    super(props);
    this.state = {
      selectedNamespace: null,
    };
  }
  eventEmitter = ee(ee);

  componentWillMount() {
    this.setNamespace();
    this.sub = NamespaceStore.subscribe(this.setNamespace.bind(this));
  }

  componentWillUnmount() {
    this.sub();
  }

  findNamespace(list, name) {
    return find(list, { name: name });
  }

  async setNamespace() {
    const selectedNamespace = await getValidNamespace();
    const isvalid = await isValidNamespace(selectedNamespace);
    if (!isvalid) {
      this.eventEmitter.emit(globalEvents.PAGE_LEVEL_ERROR, {
        statusCode: 404,
        data: GLOBALS.pageLevelErrors['INVALID-NAMESPACE'](selectedNamespace),
      });
    }
    localStorage.setItem('DefaultNamespace', selectedNamespace);
    this.setState({ selectedNamespace });
  }

  render() {
    if (!this.state.selectedNamespace) {
      return null;
    }
    return <Redirect to={`/ns/${this.state.selectedNamespace}`} />;
  }
}
