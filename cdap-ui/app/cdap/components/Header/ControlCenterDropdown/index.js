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
import { UncontrolledDropdown } from 'components/UncontrolledComponents';
import { DropdownToggle, DropdownItem } from 'reactstrap';
import CustomDropdownMenu from 'components/CustomDropdownMenu';
import classnames from 'classnames';
import DashboardLink from 'components/Header/DashboardLink';
import EntitiesLink from 'components/Header/EntitiesLink';
import ReportsLink from 'components/Header/ReportsLink';

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

  render() {
    return (
      <li className={classnames({ 'active': this.isCDAPActive() })}>
        <UncontrolledDropdown
          className="header-dropdown control-center"
        >
          <DropdownToggle caret>
            {T.translate(`${PREFIX}.label`)}
          </DropdownToggle>
          <CustomDropdownMenu>
            <EntitiesLink />
            <DashboardLink />
            <ReportsLink />
          </CustomDropdownMenu>
        </UncontrolledDropdown>
      </li>
    );
  }
}
