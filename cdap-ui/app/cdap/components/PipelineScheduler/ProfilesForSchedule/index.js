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
<<<<<<< HEAD
=======
    profiles: null,
    provisionersMap: {},
>>>>>>> 9a3a03f9abab0418bf08248e4f4dc1da7a04162a
    scheduleDetails: null,
    selectedProfile: this.props.selectedProfile,
    profileCustomizations: this.props.profileCustomizations
  };

  componentWillReceiveProps(nextProps) {
    if (nextProps.selectedProfile !== this.state.selectedProfile) {
      this.setState({
        selectedProfile: nextProps.selectedProfile,
        profileCustomizations: nextProps.profileCustomizations
      });
    }
  }

<<<<<<< HEAD
=======
  componentDidMount() {
    this.getProfiles();
    this.getProvisionersMap();
  }

  getProfiles = () => {
    MyCloudApi.list({
      namespace: getCurrentNamespace()
    })
    .subscribe(
      (profiles) => {
        this.setState({
          profiles
        });
      },
      (error) => {
        this.setState({
          error: error.response || error
        });
      }
    );
  };

  getProvisionersMap() {
    MyCloudApi
      .getProvisioners()
      .subscribe(
        (provisioners) => {
          let provisionersMap = {};
          provisioners.forEach(provisioner => {
            provisionersMap[provisioner.name] = provisioner.label;
          });
          this.setState({
            provisionersMap
          });
        },
        (error) => {
          this.setState({
            error: error.response || error
          });
        }
      );
  }

  selectProfile = (profileName, e) => {
    setSelectedProfile(profileName);
    preventPropagation(e);
  };

>>>>>>> 9a3a03f9abab0418bf08248e4f4dc1da7a04162a
  renderProfilesTable = () => {
    let isScheduled = this.props.scheduleStatus === StatusMapper.statusMap['SCHEDULED'];
    let selectedProfile = {
      name: this.state.selectedProfile,
      profileCustomizations: this.state.profileCustomizations
    };
    return (
<<<<<<< HEAD
      <ProfilesListView
        showProfilesCount={false}
        onProfileSelect={setSelectedProfile}
        disabled={isScheduled}
        selectedProfile={selectedProfile}
      />
=======
      <div className="grid-wrapper">
        <div className="grid grid-container">
          <div className="grid-header">
            <div className="grid-row">
              <div />
              <strong>Profile Name</strong>
              <strong>Provisioner</strong>
              <strong>Scope</strong>
            </div>
          </div>
          <div className="grid-body">
            {
              this.state.profiles.map(profile => {
                let isSelected = this.state.selectedProfile === profile.name;
                let provisionerName = profile.provisioner.name;
                let provisionerLabel = this.state.provisionersMap[provisionerName] || provisionerName;

                return (
                  <div
                    className={classnames("grid-row grid-link", {
                      'active': isSelected
                    })}
                    onClick={this.selectProfile.bind(this, profile.name)}
                  >
                    {
                      isSelected ?
                        <IconSVG name="icon-check" className="text-success" />
                      :
                        <div />
                    }
                    <div>{profile.name}</div>
                    <div>{provisionerLabel}</div>
                    <div>{profile.scope}</div>
                  </div>
                );
              })
            }
          </div>
        </div>
      </div>
>>>>>>> 9a3a03f9abab0418bf08248e4f4dc1da7a04162a
    );
  }

  renderProfilesDropdown = () => {
    let isScheduled = this.props.scheduleStatus === StatusMapper.statusMap['SCHEDULED'];
    let provisionerLabel;
    if (this.state.selectedProfile) {
      let provisionerName = selectedProfile.provisioner.name;
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
<<<<<<< HEAD
              <span> {`${extractProfileName(this.state.selectedProfile)}`}</span>
=======
              <span> {`${this.state.selectedProfile} (${provisionerLabel})`}</span>
>>>>>>> 9a3a03f9abab0418bf08248e4f4dc1da7a04162a
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
