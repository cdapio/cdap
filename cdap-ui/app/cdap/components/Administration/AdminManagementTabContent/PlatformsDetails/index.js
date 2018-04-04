/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import { TabContent, TabPane, Nav, NavItem, NavLink } from 'reactstrap';
import classnames from 'classnames';
import T from 'i18n-react';
import GenericDetails from 'components/Administration/AdminManagementTabContent/PlatformsDetails/Genericdetails';
import LoadingSVG from 'components/LoadingSVG';

const ADMINPREFIX = 'features.Administration.Component-Overview.headers';
require('./PlatformDetails.scss');

export default class PlatformsDetails extends Component {
  static propTypes = {
    platforms: PropTypes.arrayOf(PropTypes.object)
  };

  static defaultProps = {
    platforms: {}
  };

  state = {
    activeTab: Object.keys(this.props.platforms)[0],
    platforms: this.props.platforms
  };

  componentWillReceiveProps(nextProps) {
    let activeTab = Object.keys(nextProps.platforms)[0];
    this.setState({
      platforms: nextProps.platforms,
      activeTab
    });
  }

  toggleTab = (activeTab) => {
    this.setState({
      activeTab
    });
  };

  render() {
    if (!Object.keys(this.state.platforms).length) {
      return (
        <div className="platform-details loading">
          <LoadingSVG />
        </div>
      );
    }

    return (
      <div className="platform-details">
        <Nav tabs>
          {
            Object
              .keys(this.state.platforms)
              .map((platform, i) => {
                return (
                  <NavItem key={i}>
                    <NavLink
                      className={classnames({ active: this.state.activeTab === platform })}
                      onClick={this.toggleTab.bind(null, platform)}
                    >
                      {T.translate(`${ADMINPREFIX}.${platform}`)}
                    </NavLink>
                  </NavItem>
                );
              })
            }
        </Nav>
        <TabContent activeTab={this.state.activeTab}>
          {
            Object
              .keys(this.state.platforms)
              .map((platform, i) => {
                return (
                  <TabPane tabId={platform} key={i}>
                    <GenericDetails
                      details={this.state.platforms[platform]}
                      className={platform}
                    />
                  </TabPane>
                );
              })
          }
        </TabContent>
      </div>
    );
  }
}

