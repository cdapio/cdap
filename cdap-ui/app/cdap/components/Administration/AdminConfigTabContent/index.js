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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import ReloadSystemArtifacts from 'components/Administration/AdminConfigTabContent/ReloadSystemArtifacts';
import NamespacesAccordion from 'components/Administration/AdminConfigTabContent/NamespacesAccordion';
import SystemProfilesAccordion from 'components/Administration/AdminConfigTabContent/SystemProfilesAccordion';
import SystemPrefsAccordion from 'components/Administration/AdminConfigTabContent/SystemPrefsAccordion';
import { MyNamespaceApi } from 'api/namespace';
import { Link } from 'react-router-dom';
import Helmet from 'react-helmet';
import { Theme } from 'services/ThemeHelper';
import T from 'i18n-react';

require('./AdminConfigTabContent.scss');

const I18N_PREFIX = 'features.Administration.Configure';

export const ADMIN_CONFIG_ACCORDIONS = {
  namespaces: 'NAMESPACES',
  systemProfiles: 'SYSTEM_PROFILES',
  systemPrefs: 'SYSTEM_PREFS',
};

export default class AdminConfigTabContent extends Component {
  state = {
    namespaces: [],
    namespacesCountLoading: true,
    expandedAccordion: this.props.accordionToExpand || ADMIN_CONFIG_ACCORDIONS.namespaces,
  };

  static propTypes = {
    accordionToExpand: PropTypes.string,
  };

  static defaultProps = {
    accordionToExpand: ADMIN_CONFIG_ACCORDIONS.namespaces,
  };

  expandAccordion = (accordion) => {
    if (this.state.expandedAccordion === accordion) {
      this.setState({
        expandedAccordion: null,
      });
    } else {
      this.setState({
        expandedAccordion: accordion,
      });
    }
  };

  componentDidMount() {
    this.getNamespaces();
  }

  getNamespaces() {
    MyNamespaceApi.list().subscribe(
      (res) => {
        this.setState({
          namespaces: res,
          namespacesCountLoading: false,
        });
      },
      (err) => console.log(err)
    );
  }

  render() {
    return (
      <div className="admin-config-tab-content">
        <Helmet
          title={T.translate(`${I18N_PREFIX}.pageTitle`, {
            productName: Theme.productName,
          })}
        />
        <div className="action-buttons">
          <ReloadSystemArtifacts />
          <Link to="/httpexecutor" className="btn btn-secondary">
            {T.translate(`${I18N_PREFIX}.buttons.MakeRESTCalls.label`)}
          </Link>
        </div>
        <NamespacesAccordion
          namespaces={this.state.namespaces}
          loading={this.state.namespacesCountLoading}
          expanded={this.state.expandedAccordion === ADMIN_CONFIG_ACCORDIONS.namespaces}
          onExpand={this.expandAccordion.bind(this, ADMIN_CONFIG_ACCORDIONS.namespaces)}
        />
        <SystemProfilesAccordion
          expanded={this.state.expandedAccordion === ADMIN_CONFIG_ACCORDIONS.systemProfiles}
          onExpand={this.expandAccordion.bind(this, ADMIN_CONFIG_ACCORDIONS.systemProfiles)}
        />
        <SystemPrefsAccordion
          expanded={this.state.expandedAccordion === ADMIN_CONFIG_ACCORDIONS.systemPrefs}
          onExpand={this.expandAccordion.bind(this, ADMIN_CONFIG_ACCORDIONS.systemPrefs)}
        />
      </div>
    );
  }
}
