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
import IconSVG from 'components/IconSVG';
import {Dropdown, DropdownToggle, DropdownMenu} from 'reactstrap';
import {setSelectedProfile} from 'components/PipelineScheduler/Store/ActionCreator';
import {connect} from 'react-redux';
import StatusMapper from 'services/StatusMapper';
import ProfilesListView, {extractProfileName, isSystemProfile} from 'components/PipelineDetails/ProfilesListView';
import {MyCloudApi} from 'api/cloud';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {getProvisionersMap} from 'components/Cloud/Profiles/Store/Provisioners';
import {preventPropagation} from 'services/helpers';
require('./ProfilesForSchedule.scss');

export const PROFILES_DROPDOWN_DOM_CLASS = 'profiles-list-dropdown';

class ProfilesForSchedule extends Component {
  static propTypes = {
    selectedProfile: PropTypes.string,
    scheduleStatus: PropTypes.string,
    profileCustomizations: PropTypes.object
  };

  static defaultProps = {
    selectedProfile: null,
    profileCustomizations: {}
  }
  state = {
    scheduleDetails: null,
    provisionersMap: {},
    profileDetails: {},
    selectedProfile: this.props.selectedProfile,
    profileCustomizations: this.props.profileCustomizations,
    openProfilesDropdown: false
  };

  componentDidMount() {
    this.setProvisionersMap();
    this.setProfileDetails();
  }
  componentWillReceiveProps(nextProps) {
    this.setState({
      selectedProfile: nextProps.selectedProfile,
      profileCustomizations: nextProps.profileCustomizations
    }, () => {
      this.setProfileDetails();
    });
  }

  toggleProfileDropdown = (e) => {
    this.setState({
      openProfilesDropdown: !this.state.openProfilesDropdown
    });
    if (typeof e === 'object') {
      preventPropagation(e);
    }
  };

  setProfileDetails() {
    if (!this.state.selectedProfile) {
      return;
    }
    MyCloudApi
      .get({
        namespace: isSystemProfile(this.state.selectedProfile) ? 'system' : getCurrentNamespace(),
        profile: extractProfileName(this.state.selectedProfile)
      })
      .subscribe(profileDetails => {
        this.setState({
          profileDetails
        });
      });
  }

  setProvisionersMap() {
    getProvisionersMap().subscribe((state) => {
      this.setState({
        provisionersMap: state.nameToLabelMap
      });
    });
  }

  setSelectedProfile = (selectedProfile, profileCustomizations = {}, e) => {
    setSelectedProfile(selectedProfile, profileCustomizations);
    this.toggleProfileDropdown(e);
  };

  renderProfilesTable = () => {
    if (!this.state.openProfilesDropdown) {
      return null;
    }
    let isScheduled = this.props.scheduleStatus === StatusMapper.statusMap['SCHEDULED'];
    let selectedProfile = {
      name: this.state.selectedProfile,
      profileCustomizations: this.state.profileCustomizations
    };
    return (
      <ProfilesListView
        showProfilesCount={false}
        onProfileSelect={this.setSelectedProfile}
        disabled={isScheduled}
        selectedProfile={selectedProfile}
      />
    );
  }

  renderProfilesDropdown = () => {
    let isScheduled = this.props.scheduleStatus === StatusMapper.statusMap['SCHEDULED'];
    let provisionerLabel;
    if (this.state.selectedProfile) {
      let {profileDetails = {}} = this.state;
      let {provisioner = {}} = profileDetails;
      let {name: provisionerName} = provisioner;
      provisionerLabel = this.state.provisionersMap[provisionerName] || provisionerName;
    }

    return (
      <Dropdown
        className={PROFILES_DROPDOWN_DOM_CLASS}
        disabled={isScheduled}
        isOpen={this.state.openProfilesDropdown}
        toggle={this.toggleProfileDropdown}
      >
        <DropdownToggle
          disabled={isScheduled}
          caret
        >
          {
            this.state.selectedProfile ?
              <span>
                {
                  provisionerLabel ?
                    `${extractProfileName(this.state.selectedProfile)} (${provisionerLabel})`
                  :
                    `${extractProfileName(this.state.selectedProfile)}`
                }
              </span>
            :
              <span>Select a Profile</span>
          }
          <IconSVG name="icon-caret-down" />
        </DropdownToggle>
        <DropdownMenu>
          { this.renderProfilesTable() }
        </DropdownMenu>
      </Dropdown>
    );
  };

  render() {
    return (
      <div className="form-group row">
        <label className="col-xs-3 control-label">
          Compute Profiles
        </label>
        <div className="col-xs-6 schedule-values-container">
          {this.renderProfilesDropdown()}
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    selectedProfile: state.profiles.selectedProfile,
    profileCustomizations: state.profiles.profileCustomizations,
    scheduleStatus: state.scheduleStatus
  };
};

const ConnectedProfilesForSchedule = connect(mapStateToProps)(ProfilesForSchedule);

export default ConnectedProfilesForSchedule;
