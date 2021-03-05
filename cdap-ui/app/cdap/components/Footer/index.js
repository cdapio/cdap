/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
import { Theme } from 'services/ThemeHelper';
import If from '../If';
import { objectQuery } from 'services/helpers';
import NamespaceStore from 'services/NamespaceStore';
require('./Footer.scss');

class Footer extends Component {
  state = {
    currentNamespace: NamespaceStore.getState().selectedNamespace,
  };

  componentWillMount() {
    this.namespaceStoreSub = NamespaceStore.subscribe(() => {
      this.setState({
        currentNamespace: NamespaceStore.getState().selectedNamespace,
      });
    });
  }

  render() {
    const footerText = Theme.footerText;
    const footerUrl = Theme.footerLink;
    // 'project-id-30-characters-name1/instance-id-30-characters-name';
    const instanceMetadataId = objectQuery(window, 'CDAP_CONFIG', 'instanceMetadataId');
    return (
      <footer className="app-footer">
        <p className="selected-namespace">Namespace: {this.state.currentNamespace}</p>
        <p className="text-center text-muted">
          <a href={footerUrl} target="_blank" rel="noopener noreferrer">
            {footerText}
          </a>
        </p>
        <If condition={instanceMetadataId}>
          <p className="instance-metadata-id">Instance Id: {instanceMetadataId}</p>
        </If>
      </footer>
    );
  }
}

export default Footer;
