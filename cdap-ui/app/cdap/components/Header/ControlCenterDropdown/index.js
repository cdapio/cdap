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
import T from 'i18n-react';
import NavLinkWrapper from 'components/NavLinkWrapper';
import { UncontrolledDropdown } from 'components/UncontrolledComponents';
import { DropdownToggle, DropdownItem } from 'reactstrap';
import CustomDropdownMenu from 'components/CustomDropdownMenu';
import {
  hideDashboard,
  hideReports
} from 'services/ThemeHelper';

require('./ControlCenterDropdown.scss');

const PREFIX = 'features.Navbar.ControlCenter';

export default class ControlCenterDropdown extends Component {
  static propTypes = {
    nativeLink: PropTypes.bool,
    namespace: PropTypes.string
  };

  static defaultProps = {
    nativeLink: false
  };

  isCDAPActive = () => {
    let location = window.location;

    let basePath = `/cdap/ns/${this.props.namespace}`;

    let dataprepBasePath = `${basePath}/dataprep`;
    let connectionsBasePath = `${basePath}/connections`;
    let rulesenginepath = `${basePath}/rulesengine`;
    let analytics = `${basePath}/experiments`;
    let namespaceDetails = `${basePath}/details`;
    let createProfile = `${basePath}/profiles/create`;
    let profileDetails = `${basePath}/profiles/details`;

    if (
      location.pathname.startsWith(basePath) &&
      !location.pathname.startsWith(dataprepBasePath) &&
      !location.pathname.startsWith(connectionsBasePath) &&
      !location.pathname.startsWith(rulesenginepath) &&
      !location.pathname.startsWith(analytics) &&
      !location.pathname.startsWith(namespaceDetails) &&
      !location.pathname.startsWith(createProfile) &&
      !location.pathname.startsWith(profileDetails)
    ) {
      return true;
    }
    return false;
  };

  isDashboardActive = (match, location = window.location) => {
    if (match && match.isExact) {
      return true;
    }

    let path = `/ns/${this.props.namespace}/operations`;

    if (location.pathname.startsWith(path)) {
      return true;
    }

    return false;
  };

  isReportsActive = (match, location = window.location) => {
    if (match && match.isExact) {
      return true;
    }

    let path = `/ns/${this.props.namespace}/reports`;

    if (location.pathname.startsWith(path)) {
      return true;
    }

    return false;
  };

  isEntitiesActive = (match, location = window.location) => {
    if (match && match.isExact) {
      return true;
    }

    if (
      this.isCDAPActive() &&
      !this.isDashboardActive(match, location) &&
      !this.isReportsActive(match, location)
    ) {
      return true;
    }

    return false;
  };

  renderEntitiesLink() {
    const baseCDAPURL = `/ns/${this.props.namespace}`;
    return (
      <DropdownItem tag="li">
        <NavLinkWrapper
          isNativeLink={this.props.nativeLink}
          to={this.props.nativeLink ? `/cdap${baseCDAPURL}` : baseCDAPURL}
          isActive={this.isEntitiesActive}
        >
          {T.translate(`${PREFIX}.entities`)}
        </NavLinkWrapper>
      </DropdownItem>
    );
  }

  renderDashboardLink() {
    if (hideDashboard()) {
      return null;
    }

    const dashboardURL = `/ns/${this.props.namespace}/operations`;
    return (
      <DropdownItem tag="li">
        <NavLinkWrapper
          isNativeLink={this.props.nativeLink}
          to={this.props.nativeLink ? `/cdap${dashboardURL}` : dashboardURL}
          isActive={this.isDashboardActive}
        >
          {T.translate(`${PREFIX}.dashboard`)}
        </NavLinkWrapper>
      </DropdownItem>
    );
  }

  renderReportsLink() {
    if (hideReports()) {
      return null;
    }

    const reportsURL = `/ns/${this.props.namespace}/reports`;
    return (
      <DropdownItem tag="li">
        <NavLinkWrapper
          isNativeLink={this.props.nativeLink}
          to={this.props.nativeLink ? `/cdap${reportsURL}` : reportsURL}
          isActive={this.isReportsActive}
        >
          {T.translate(`${PREFIX}.reports`)}
        </NavLinkWrapper>
      </DropdownItem>
    );
  }

  render() {
    return (
      <UncontrolledDropdown
        className="header-dropdown control-center"
      >
        <DropdownToggle caret>
          {T.translate(`${PREFIX}.label`)}
        </DropdownToggle>
        <CustomDropdownMenu>
          {this.renderEntitiesLink()}
          {this.renderDashboardLink()}
          {this.renderReportsLink()}
        </CustomDropdownMenu>
      </UncontrolledDropdown>
    );
  }
}
