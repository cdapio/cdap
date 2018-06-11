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
import ProfileCustomizePopover from 'components/PipelineDetails/ProfilesListView/ProfileCustomizePopover';
import {isNilOrEmpty} from 'services/helpers';
import isEqual from 'lodash/isEqual';
import {getCustomizationMap} from 'components/PipelineConfigurations/Store/ActionCreator';
import {getProvisionersMap} from 'components/Cloud/Profiles/Store/Provisioners';
require('./ProfilesListViewInPipeline.scss');

export const PROFILE_NAME_PREFERENCE_PROPERTY = 'system.profile.name';
export const PROFILE_PROPERTIES_PREFERENCE = 'system.profile.properties';
export const extractProfileName = (name = '') => name.replace(/(user|system):/g, '');
export const isSystemProfile = (name = '') => name.indexOf('system:') === 0;
// NOTE: This is never actually saved to backend. This is hardcoded here until we figure out a 
// clean way to add `system.profiles.name` to namespace preference. If there is no `system.profiles.name` set
// at namespace or app level UI show "default" profile selected
export const DEFAULT_PROFILE_NAME = 'system:default';

export default class ProfilesListViewInPipeline extends Component {

  static propTypes = {
    onProfileSelect: PropTypes.func,
    selectedProfile: PropTypes.object,
    tableTitle: PropTypes.string,
    showProfilesCount: PropTypes.bool,
    disabled: PropTypes.bool,
    provisionersMap: PropTypes.object
  };

  static defaultProps = {
    selectedProfile: {},
    showProfilesCount: true,
    disabled: false
  };

  state = {
    profiles: [],
    provisionersMap: {},
    loading: true,
    selectedProfile: this.props.selectedProfile.name || '',
    profileCustomizations: this.props.selectedProfile.profileCustomizations || {}
  };

  componentWillReceiveProps(nextProps) {
    if (
      this.state.selectedProfile !== nextProps.selectedProfile.name ||
      !isEqual(nextProps.selectedProfile.profileCustomizations, this.state.customizations)
    ) {
      this.setState({
        selectedProfile: nextProps.selectedProfile.name,
        profileCustomizations: nextProps.selectedProfile.profileCustomizations
      });
    }
  }

  componentWillMount() {
    let appId = PipelineDetailStore.getState().name;
    Observable.forkJoin(
      MyCloudApi.list({ namespace: getCurrentNamespace() }),
      MyCloudApi.list({ namespace: 'system' }),
      MyPreferenceApi.getAppPreferencesResolved({
        namespace: getCurrentNamespace(),
        appId
      })
    )
      .subscribe(
        ([profiles = [], systemProfiles = [], preferences = {}]) => {
          let selectedProfile = this.state.selectedProfile || preferences[PROFILE_NAME_PREFERENCE_PROPERTY] || DEFAULT_PROFILE_NAME;
          let profileCustomizations = isNilOrEmpty(this.state.profileCustomizations) ?
            getCustomizationMap(preferences)
          :
            this.state.profileCustomizations;

          let allProfiles = profiles.concat(systemProfiles);
          // This is to surface the selected profile to the top
          // instead of hiding somewhere in the bottom.
          let selectedProfileName = extractProfileName(selectedProfile);
          let sortedProfiles = [];
          allProfiles.forEach(profile => {
            if (profile.name === selectedProfileName) {
              sortedProfiles.unshift(profile);
            } else {
              sortedProfiles.push(profile);
            }
          });
          this.setState({
            loading: false,
            profiles: sortedProfiles,
            selectedProfile,
            profileCustomizations
          });
        },
        (err) => {
          console.log('ERROR in fetching profiles from backend: ', err);
        }
      );
  }

  componentDidMount() {
    getProvisionersMap().subscribe((state) => {
      this.setState({
        provisionersMap: state.nameToLabelMap
      });
    });
  }

  onProfileSelect = (profileName, customizations = {}, e) => {
    if (this.props.disabled) {
      return;
    }
    this.setState({
      selectedProfile: profileName
    });
    if (this.props.onProfileSelect) {
      this.props.onProfileSelect(profileName, customizations, e);
    }
  };

  onProfileSelectWithoutCustomization = (profileName, e) => {
    this.onProfileSelect(profileName, {}, e);
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
          <strong />
        </div>
      </div>
    );
  };

  renderProfileRow = (profile) => {
    let profileNamespace = profile.scope === 'SYSTEM' ? 'system' : getCurrentNamespace();
    let profileDetailsLink = `${location.protocol}//${location.host}/cdap/ns/${profileNamespace}/profiles/details/${profile.name}`;
    let profileName = profile.scope === 'SYSTEM' ? `system:${profile.name}` : `user:${profile.name}`;
    let selectedProfile = this.state.selectedProfile || '';
    selectedProfile = extractProfileName(selectedProfile);
    let provisionerName = profile.provisioner.name;
    let provisionerLabel = this.state.provisionersMap[provisionerName] || provisionerName;
    return (
      <div
        key={profileName}
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
        <div onClick={this.onProfileSelectWithoutCustomization.bind(this, profileName)}>
          {
            this.state.selectedProfile === profileName ? (
              <IconSVG name="icon-check" className="text-success" />
            ) : null
          }
        </div>
        <div onClick={this.onProfileSelectWithoutCustomization.bind(this, profileName)}>{profile.name}</div>
        <div onClick={this.onProfileSelectWithoutCustomization.bind(this, profileName)}>{provisionerLabel}</div>
        <div onClick={this.onProfileSelectWithoutCustomization.bind(this, profileName)}>{profile.scope}</div>
        <div>
          <a href={profileDetailsLink}> View </a>
        </div>
        <ProfileCustomizePopover
          profile={profile}
          onProfileSelect={this.onProfileSelect}
          disabled={this.props.disabled}
          customizations={
            profile.name === selectedProfile ?
              this.state.profileCustomizations
            :
              {}
          }
        />
      </div>
    );
  }

  renderGridBody = () => {
    if (this.props.disabled) {
      let selectedProfileName = extractProfileName(this.state.selectedProfile);
      let match = this.state.profiles.filter(profile => profile.name === selectedProfileName);
      if (match.length) {
        return match.map(this.renderProfileRow);
      }
    }

    return (
      <div className="grid-body">
        {
          this.state.profiles.map(this.renderProfileRow)
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
          this.props.disabled || !this.props.showProfilesCount ?
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
      let selectedProfileName = extractProfileName(this.state.selectedProfile);
      let match = this.state.profiles.filter(profile => profile.name === selectedProfileName);
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
