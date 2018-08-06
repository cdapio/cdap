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
import {PROFILE_STATUSES} from 'components/Cloud/Profiles/Store';
import {extractProfileName, getProfileNameWithScope} from 'components/Cloud/Profiles/Store/ActionCreator';
import {CLOUD, SCOPES, SYSTEM_NAMESPACE} from 'services/global-constants';
import T from 'i18n-react';

const PREFIX = 'features.PipelineDetails.ProfilesListView';

require('./ProfilesListViewInPipeline.scss');

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
    defaultProfile: '',
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
    let namespace = getCurrentNamespace();

    Observable.forkJoin(
      MyCloudApi.list({ namespace: getCurrentNamespace() }),
      MyCloudApi.getSystemProfiles(),
      MyPreferenceApi.getAppPreferencesResolved({
        namespace,
        appId
      }),
      MyPreferenceApi.getNamespacePreferencesResolved({namespace})
    )
      .subscribe(
        ([profiles = [], systemProfiles = [], preferences = {}, namespacePreferences = {}]) => {
          let profileCustomizations = isNilOrEmpty(this.state.profileCustomizations) ?
            getCustomizationMap(preferences)
          :
            this.state.profileCustomizations;

          let allProfiles = profiles.concat(systemProfiles);

          let defaultProfile = namespacePreferences[CLOUD.PROFILE_NAME_PREFERENCE_PROPERTY] || CLOUD.DEFAULT_PROFILE_NAME;

          let selectedProfile = this.state.selectedProfile || preferences[CLOUD.PROFILE_NAME_PREFERENCE_PROPERTY] || defaultProfile;
          let selectedProfileName = extractProfileName(selectedProfile);


          // This is to surface the selected profile to the top
          // instead of hiding somewhere in the bottom.
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
            defaultProfile,
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

  onProfileSelectWithoutCustomization = (profileName, profileIsEnabled, e) => {
    if (!profileIsEnabled) {
      return;
    }

    this.onProfileSelect(profileName, {}, e);
  };

  renderGridHeader = () => {
    return (
      <div className="grid-header">
        <div className="grid-row">
          <div></div>
          <strong>{T.translate('features.Cloud.Profiles.ListView.profileName')}</strong>
          <strong>{T.translate('features.Cloud.Profiles.common.provisioner')}</strong>
          <strong>{T.translate('commons.scope')}</strong>
          <strong>{T.translate('commons.status')}</strong>
          <strong />
          <strong />
        </div>
      </div>
    );
  };

  renderProfileRow = (profile) => {
    let profileNamespace = profile.scope === SCOPES.SYSTEM ? SYSTEM_NAMESPACE : getCurrentNamespace();
    let profileDetailsLink = `${location.protocol}//${location.host}/cdap/ns/${profileNamespace}/profiles/details/${profile.name}`;
    let profileName = getProfileNameWithScope(profile.name, profile.scope);
    let selectedProfile = this.state.selectedProfile || '';
    selectedProfile = extractProfileName(selectedProfile);
    let provisionerName = profile.provisioner.name;
    let provisionerLabel = this.state.provisionersMap[provisionerName] || provisionerName;
    const profileStatus = PROFILE_STATUSES[profile.status];
    const profileIsEnabled = profileStatus === 'enabled';
    const onProfileSelectHandler = this.onProfileSelectWithoutCustomization.bind(this, profileName, profileIsEnabled);
    const profileLabel = profile.label || profile.name;

    const CustomizeLabel = () => {
      if (!profileIsEnabled) {
        return <div>Customize</div>;
      }
      return (
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
      );
    };

    const renderDefaultProfileStar = (profileName) => {
      if (profileName !== this.state.defaultProfile) {
        return null;
      }
      return <IconSVG name="icon-star" />;
    };

    return (
      <div
        key={profileName}
        className={classnames(`grid-row grid-link ${profileStatus}`, {
          "active": this.state.selectedProfile === profileName
        })}
      >
        {
          /*
            There is an onClick handler on each cell except the last one.
            This is to prevent the user from selecting a profile while trying to click on the details link
          */
        }
        <div onClick={onProfileSelectHandler}>
          {
            this.state.selectedProfile === profileName ? (
              <IconSVG name="icon-check" className="text-success" />
            ) : null
          }
        </div>
        <div
          className="profile-label"
          title={profileLabel}
          onClick={onProfileSelectHandler}
        >
          {renderDefaultProfileStar(profileName)}
          {profileLabel}
        </div>
        <div onClick={onProfileSelectHandler}>
          {provisionerLabel}
        </div>
        <div onClick={onProfileSelectHandler}>
          {profile.scope}
        </div>
        <div
          className="profile-status"
          onClick={onProfileSelectHandler}
        >
          {T.translate(`features.Cloud.Profiles.common.${profileStatus}`)}
        </div>
        <CustomizeLabel />
        <div>
          <a href={profileDetailsLink}> View </a>
        </div>
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
          <strong>{T.translate(`${PREFIX}.noProfiles`)}</strong>
          <div>
            <a href={`/cdap/ns/${getCurrentNamespace()}/profiles/create`}>
              {T.translate(`${PREFIX}.click`)}
            </a>
            <span>{T.translate(`${PREFIX}.toCreate`)}</span>
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
            <div className="profiles-count text-right">
              {T.translate(`${PREFIX}.countProfiles`, {
                context: this.state.profiles.length
              })}
            </div>
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
