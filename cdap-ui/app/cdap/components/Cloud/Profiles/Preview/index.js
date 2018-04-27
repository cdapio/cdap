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
import {MyCloudApi} from 'api/cloud';
import {getCurrentNamespace} from 'services/NamespaceStore';
import LoadingSVG from 'components/LoadingSVG';
import IconSVG from 'components/IconSVG';
require('./Preview.scss');

export default class ProfilePreview extends Component {
  static propTypes = {
    profileScope: PropTypes.string,
    profileName: PropTypes.string,
    profileCustomProperties: PropTypes.object
  };
  state = {
    profileDetails: null,
    loading: true,
    error: null
  };

  componentDidMount() {
    let namespace = getCurrentNamespace();
    if (this.props.profileScope === 'system') {
      namespace = 'system';
    }
    MyCloudApi.get({
      namespace,
      profile: this.props.profileName
    })
    .subscribe(
      profileDetails => {
        this.setState({
          profileDetails,
          loading: false
        });
      },
      error => {
        this.setState({
          error,
          loading: false
        });
      }
    );
  }

  render() {
    if (this.state.loading) {
      return (
        <div className="profile-preview text-xs-center">
          <LoadingSVG />
        </div>
      );
    }
    let profileNamespace = this.state.profileDetails.scope === 'SYSTEM' ? 'system' : getCurrentNamespace();
    let profileDetailsLink = `${location.protocol}//${location.host}/cdap/ns/${profileNamespace}/profiles/details/${this.props.profileName}`;
    return (
      <div className="profile-preview text-xs-left">
        <strong>{this.props.profileName}</strong>
        <div className="profile-descripion">
          <p className="multi-line-text">
            {this.state.profileDetails.description}
          </p>
        </div>
        <div className="grid grid-container">
          <div className="grid-header">
            <div className="grid-row">
              <div>Provisioner</div>
              <div>Scope</div>
              <div>Last 24hr # runs</div>
              <div>Last 24hr node hr</div>
              <div>Creation Date</div>
            </div>
          </div>
          <div className="grid-body">
            <div className="grid-row">
              <div>
                <IconSVG name="icon-cloud" />
                <span className="provisioner-name truncate-text">
                  {this.state.profileDetails.provisioner.name}
                </span>
              </div>
              <div className="truncate-text">
                {this.state.profileDetails.scope}
              </div>
              <div />
              <div />
              <div />
            </div>
          </div>
        </div>
        <hr />
        <div>
          <a href={profileDetailsLink}>
            View Details
          </a>
        </div>
      </div>
    );
  }
}
