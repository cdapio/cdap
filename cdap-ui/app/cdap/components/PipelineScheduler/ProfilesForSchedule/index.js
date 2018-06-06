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
import {UncontrolledDropdown} from 'components/UncontrolledComponents';
import IconSVG from 'components/IconSVG';
import {DropdownToggle, DropdownMenu} from 'reactstrap';
import {setSelectedProfile} from 'components/PipelineScheduler/Store/ActionCreator';
import {connect} from 'react-redux';
import StatusMapper from 'services/StatusMapper';
import ProfilesListView, {extractProfileName} from 'components/PipelineDetails/ProfilesListView';
import isEmpty from 'lodash/isEmpty';
import {MyCloudApi} from 'api/cloud';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {getProvisionersMap, fetchProvisioners} from 'components/Cloud/Profiles/Store/Provisioners';
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
    profileCustomizations: this.props.profileCustomizations
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

  setProfileDetails() {
    if (this.state.selectedProfile) {
      MyCloudApi
        .get({
          namespace: getCurrentNamespace(),
          profile: extractProfileName(this.state.selectedProfile)
        })
        .subscribe(profileDetails => {
          this.setState({
            profileDetails
          });
        });
    }
  }

  setProvisionersMap() {
    if (isEmpty(getProvisionersMap().nameToLabelMap)) {
      fetchProvisioners().subscribe(() => {
        this.setState({
          provisionersMap: getProvisionersMap().nameToLabelMap
        });
      });
    } else {
      this.setState({
        provisionersMap: getProvisionersMap().nameToLabelMap
      });
    }
  }

  renderProfilesTable = () => {
    let isScheduled = this.props.scheduleStatus === StatusMapper.statusMap['SCHEDULED'];
    let selectedProfile = {
      name: this.state.selectedProfile,
      profileCustomizations: this.state.profileCustomizations
    };
    return (
      <ProfilesListView
        showProfilesCount={false}
        onProfileSelect={setSelectedProfile}
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
      <UncontrolledDropdown
        className={PROFILES_DROPDOWN_DOM_CLASS}
        tether={{}} /* Apparently this attaches it to the body */
        disabled={isScheduled}
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
      </UncontrolledDropdown>
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
