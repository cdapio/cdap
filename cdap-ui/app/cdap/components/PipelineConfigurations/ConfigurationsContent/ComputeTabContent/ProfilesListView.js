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

import PropTypes from 'prop-types';
import React, {Component} from 'react';
import {MyCloudApi} from 'api/cloud';
import {getCurrentNamespace} from 'services/NamespaceStore';
import LoadingSVG from 'components/LoadingSVG';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import {MyPreferenceApi} from 'api/preference';
import {Observable} from 'rxjs/Observable';
import PipelineDetailStore from 'components/PipelineDetails/store';

require('./ListViewInPipeline.scss');

export const PROFILE_NAME_PREFERENCE_PROPERTY = 'system.profile.name';
export default class ProfilesListViewInPipeline extends Component {

  static propTypes = {
    onProfileSelect: PropTypes.func,
    selectedProfile: PropTypes.object,
    tableTitle: PropTypes.string,
    disabled: PropTypes.bool
  };

  static defaultProps = {
    selectedProfile: {},
    disabled: false
  };

  state = {
    profiles: [],
    loading: true,
    selectedProfile: this.props.selectedProfile.name || null
  };

  componentWillReceiveProps(nextProps) {
    if (this.state.selectedProfile !== nextProps.selectedProfile.name) {
      this.setState({
        selectedProfile: nextProps.selectedProfile.name
      });
    }
  }

  componentWillMount() {
    let appId = PipelineDetailStore.getState().name;
    Observable.forkJoin(
      MyCloudApi.list({ namespace: getCurrentNamespace() }),
      MyCloudApi.list({ namespace: 'system' }),
      MyPreferenceApi.getAppPreferences({
        namespace: getCurrentNamespace(),
        appId
      })
    )
      .subscribe(
        ([profiles = [], systemProfiles = [], preferences = {}]) => {
          let selectedProfile = preferences[PROFILE_NAME_PREFERENCE_PROPERTY] || this.state.selectedProfile;
          let allProfiles = profiles.concat(systemProfiles);
          this.setState({
            loading: false,
            profiles: allProfiles,
            selectedProfile
          });
        },
        (err) => {
          console.log('ERROR in fetching profiles from backend: ', err);
        }
      );
  }

  onProfileSelect = (profileName) => {
    if (this.props.disabled) {
      return;
    }
    this.setState({
      selectedProfile: profileName
    });
    if (this.props.onProfileSelect) {
      this.props.onProfileSelect(profileName);
    }
  };

  renderGridHeader = () => {
    return (
      <div className="grid-header">
        <div className="grid-row">
          <div></div>
          <strong>Profile Name</strong>
          <strong>Provisioner</strong>
          <strong>Scope</strong>
          <strong />
        </div>
      </div>
    );
  };

  renderGridBody = () => {
    if (this.props.disabled) {
      let match = this.state.profiles.filter(profile => profile.name === this.state.selectedProfile);
      if (match.length) {
        return (
          match.map(profile => {
            let profileNamespace = profile.scope === 'SYSTEM' ? 'system' : getCurrentNamespace();
            let profileDetailsLink = `${location.protocol}//${location.host}/cdap/ns/${profileNamespace}/profiles/details/${profile.name}`;
            let profileName = profile.scope === 'SYSTEM' ? `system:${profile.name}` : `user:${profile.name}`;
            return (
              <div
                className={classnames("grid-row grid-link", {
                  "active": this.state.selectedProfile === profileName
                })}
              >
                <div onClick={this.onProfileSelect.bind(this, profileName)}>
                  {
                    this.state.selectedProfile === profileName ? (
                      <IconSVG name="icon-check" className="text-success" />
                    ) : null
                  }
                </div>
                <div onClick={this.onProfileSelect.bind(this, profileName)}>{profile.name}</div>
                <div onClick={this.onProfileSelect.bind(this, profileName)}>{profile.provisioner.name}</div>
                <div onClick={this.onProfileSelect.bind(this, profileName)}>{profile.scope}</div>
                <div>
                  <a href={profileDetailsLink}>
                    View
                  </a>
                </div>
              </div>
            );
          })
        );
      }
    }
    return (
      <div className="grid-body">
        {
          this.state.profiles.map(profile => {
            let profileNamespace = profile.scope === 'SYSTEM' ? 'system' : getCurrentNamespace();
            let profileDetailsLink = `${location.protocol}//${location.host}/cdap/ns/${profileNamespace}/profiles/details/${profile.name}`;
            let profileName = profile.scope === 'SYSTEM' ? `system:${profile.name}` : `user:${profile.name}`;
            return (
              <div
                className={classnames("grid-row grid-link", {
                  "active": this.state.selectedProfile === profileName
                })}
              >
                {
                  /*
                    There is an onClick handler on each cell except the last one.
                    This is to prevent the user from selecting a profile while trying to click on the details link
                  */
                }
                <div onClick={this.onProfileSelect.bind(this, profileName)}>
                  {
                    this.state.selectedProfile === profileName ? (
                      <IconSVG name="icon-check" className="text-success" />
                    ) : null
                  }
                </div>
                <div onClick={this.onProfileSelect.bind(this, profileName)}>{profile.name}</div>
                <div onClick={this.onProfileSelect.bind(this, profileName)}>{profile.provisioner.name}</div>
                <div onClick={this.onProfileSelect.bind(this, profileName)}>{profile.scope}</div>
                <div>
                  <a href={profileDetailsLink}> View </a>
                </div>
              </div>
            );
          })
        }
      </div>
    );
  };

  renderGrid = () => {
    if (!this.state.profiles.length) {
      return (
        <div>
          <strong> No Profiles created </strong>
          <div>
            <a href={`/cdap/ns/${getCurrentNamespace()}/profiles/create`}>
              Click here
            </a>
            <span> to create one </span>
          </div>
        </div>
      );
    }
    return (
      <div className="profiles-listview grid-wrapper">
        <strong>{this.props.tableTitle}</strong>
        {
          this.props.disabled ?
            null
          :
            <div className="profiles-count text-right">{this.state.profiles.length} Compute Profiles</div>
        }
        <div className={classnames('grid grid-container', {
          disabled: this.props.disabled
        })}>
          {this.renderGridHeader()}
          {this.renderGridBody()}
        </div>
      </div>
    );
  };

  render() {
    if (this.state.loading) {
      return (
        <div>
          <LoadingSVG />
        </div>
      );
    }
    if (this.props.disabled) {
      let match = this.state.profiles.filter(profile => profile.name === this.state.selectedProfile);
      if (!match.length) {
        return (
          <div className="profiles-list-view-on-pipeline empty-container">
            <h4>No Profile selected</h4>
          </div>
        );
      }
    }
    return (
      <div className="profiles-list-view-on-pipeline">
        {this.renderGrid()}
      </div>
    );
  }
}
