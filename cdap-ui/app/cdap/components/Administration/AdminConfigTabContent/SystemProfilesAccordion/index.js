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

import React, {Component} from 'react';
import PropTypes from 'prop-types';
import {Link} from 'react-router-dom';
import ProfilesListView from 'components/Cloud/Profiles/ListView';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
require('./SystemProfilesAccordion.scss');

const PREFIX = 'features.Administration.Accordions.SystemProfiles';

export default class SystemProfilesAccordion extends Component {
  state = {
    profilesCount: this.props.profiles.length
  };

  static propTypes = {
    profiles: PropTypes.array,
    loading: PropTypes.bool,
    expanded: PropTypes.bool,
    onExpand: PropTypes.func
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.profiles.length !== this.props.profiles.length) {
      this.setState({
        profilesCount: nextProps.profiles.length
      });
    }
  }

  onChange = (profiles) => {
    this.setState({
      profilesCount: profiles.length
    });
  }

  renderLabel() {
    return (
      <div
        className="admin-config-container-toggle"
        onClick={this.props.onExpand}
      >
        <span className="admin-config-container-label">
          <IconSVG name={this.props.expanded ? "icon-caret-down" : "icon-caret-right"} />
          {
            this.props.loading ?
              (
                <h5>
                  {T.translate(`${PREFIX}.label`)}
                  <IconSVG name="icon-spinner" className="fa-spin" />
                </h5>
              )
            :
              <h5>{T.translate(`${PREFIX}.labelWithCount`, {count: this.state.profilesCount})}</h5>
          }
        </span>
        <span className="admin-config-container-description">
          {T.translate(`${PREFIX}.description`)}
        </span>
      </div>
    );
  }

  renderContent() {
    if (!this.props.expanded) {
      return null;
    }

    return (
      <div className="admin-config-container-content system-profiles-container-content">
        <Link
          className="btn btn-secondary"
          to='/create-profile'
        >
          {T.translate(`${PREFIX}.create`)}
        </Link>
        <ProfilesListView
          namespace='system'
          onChange={this.onChange}
        />
      </div>
    );
  }

  render() {
    return (
      <div className={classnames(
        "admin-config-container system-profiles-container",
        {"expanded": this.props.expanded}
      )}>
        {this.renderLabel()}
        {this.renderContent()}
      </div>
    );
  }
}
