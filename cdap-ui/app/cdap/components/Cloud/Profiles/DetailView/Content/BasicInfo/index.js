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
import {getCurrentNamespace} from 'services/NamespaceStore';
import ConfirmationModal from 'components/ConfirmationModal';
import {Redirect} from 'react-router-dom';
import {ADMIN_CONFIG_ACCORDIONS} from 'components/Administration/AdminConfigTabContent';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import ActionsPopover from 'components/Cloud/Profiles/ActionsPopover';
import isEqual from 'lodash/isEqual';
import {getProvisionerLabel, extractProfileName, getNodeHours, deleteProfile} from 'components/Cloud/Profiles/Store/ActionCreator';
import ProfileStatusToggle from 'components/Cloud/Profiles/DetailView/Content/BasicInfo/ProfileStatusToggle';
import {CLOUD, SYSTEM_NAMESPACE} from 'services/global-constants';
import {humanReadableDate} from 'services/helpers';
import CopyableId from 'components/CopyableID';

require('./BasicInfo.scss');

const PREFIX = 'features.Cloud.Profiles';

export default class ProfileDetailViewBasicInfo extends Component {
  state = {
    deleteModalOpen: false,
    deleteErrMsg: '',
    extendedDeleteErrMsg: '',
    deleteLoading: false,
    redirectToListView: false,
    provisionerLabel: getProvisionerLabel(this.props.profile, this.props.provisioners),
    oneDayMetrics: {
      runs: this.props.oneDayMetrics.runs,
      minutes: this.props.oneDayMetrics.minutes
    },
    overallMetrics: {
      runs: this.props.overallMetrics.runs,
      minutes: this.props.overallMetrics.minutes
    }
  };

  static propTypes = {
    profile: PropTypes.object,
    provisioners: PropTypes.array,
    isSystem: PropTypes.bool,
    toggleProfileStatusCallback: PropTypes.func,
    oneDayMetrics: PropTypes.shape({
      runs: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
      minutes: PropTypes.oneOfType([PropTypes.string, PropTypes.number])
    }),
    overallMetrics: PropTypes.shape({
      runs: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
      minutes: PropTypes.oneOfType([PropTypes.string, PropTypes.number])
    })
  };

  componentWillReceiveProps(nextProps) {
    if (!isEqual(nextProps.provisioners, this.props.provisioners)) {
      this.setState({
        provisionerLabel: getProvisionerLabel(nextProps.profile, nextProps.provisioners)
      });
    }
    let {oneDayMetrics, overallMetrics} = nextProps;
    this.setState({
      oneDayMetrics,
      overallMetrics
    });
  }

  toggleDeleteModal = () => {
    this.setState({
      deleteModalOpen: !this.state.deleteModalOpen,
      deleteErrMsg: '',
      extendedDeleteErrMsg: ''
    });
  };

  deleteProfile = () => {
    this.setState({
      deleteLoading: true
    });

    let namespace = this.props.isSystem ? SYSTEM_NAMESPACE : getCurrentNamespace();
    deleteProfile(namespace, this.props.profile.name, getCurrentNamespace())
      .subscribe(
        () => {
          this.setState({
            redirectToListView: true,
            deleteLoading: false
          });
        },
        (err) => {
          this.setState({
            deleteErrMsg: T.translate(`${PREFIX}.common.deleteError`),
            extendedDeleteErrMsg: err,
            deleteLoading: false
          });
        });
  };

  renderDeleteConfirmationModal() {
    if (!this.state.deleteModalOpen) {
      return null;
    }

    const confirmationText = T.translate(`${PREFIX}.common.deleteConfirmation`, {profile: this.props.profile.name});
    const confirmationElem = (
      <span title={this.props.profile.name}>
        {confirmationText}
      </span>
    );

    return (
      <ConfirmationModal
        headerTitle={T.translate(`${PREFIX}.common.deleteTitle`)}
        toggleModal={this.toggleDeleteModal}
        confirmationElem={confirmationElem}
        confirmButtonText={T.translate('commons.delete')}
        confirmFn={this.deleteProfile}
        cancelFn={this.toggleDeleteModal}
        isOpen={this.state.deleteModalOpen}
        errorMessage={this.state.deleteErrMsg}
        extendedMessage={this.state.extendedDeleteErrMsg}
        isLoading={this.state.deleteLoading}
      />
    );
  }

  renderProfileInfoGrid() {
    let profile = this.props.profile;
    return (
      <div className="grid-wrapper profile-info-grid">
        <div className="grid grid-container">
          <div className="grid-header">
            <div className="grid-row">
              <strong>{T.translate(`${PREFIX}.common.provisioner`)}</strong>
              <strong>{T.translate('commons.scope')}</strong>
              <strong>{T.translate(`${PREFIX}.common.last24HrRuns`)}</strong>
              <strong>{T.translate(`${PREFIX}.common.totalRuns`)}</strong>
              <strong>{T.translate(`${PREFIX}.common.last24HrNodeHr`)}</strong>
              <strong>{T.translate(`${PREFIX}.common.totalNodeHr`)}</strong>
              <strong>{T.translate(`${PREFIX}.DetailView.creation`)}</strong>
            </div>
          </div>
          <div className="grid-body">
            <div className="grid-row">
              <div>
                <IconSVG name="icon-cloud" />
                <span>{this.state.provisionerLabel}</span>
              </div>
              <div>{profile.scope}</div>
              <div>{this.state.oneDayMetrics.runs}</div>
              <div>{this.state.overallMetrics.runs}</div>
              <div>{getNodeHours(this.state.oneDayMetrics.minutes || '--')}</div>
              <div>{getNodeHours(this.state.overallMetrics.minutes || '--')}</div>
              <div>{humanReadableDate(profile.created, false)}</div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  renderDivider(isNativeProfile) {
    if (isNativeProfile) {
      return null;
    }
    return <span className="divider"></span>;
  }

  render() {
    let profile = this.props.profile;
    let redirectToObj = this.props.isSystem ? {
      pathname: '/administration/configuration',
      state: { accordionToExpand: ADMIN_CONFIG_ACCORDIONS.systemProfiles }
    } : `/ns/${getCurrentNamespace()}/details`;
    let namespace = this.props.isSystem ? SYSTEM_NAMESPACE : getCurrentNamespace();
    const isNativeProfile = profile.name === extractProfileName(CLOUD.DEFAULT_PROFILE_NAME);

    const actionsElem = () => {
      return (
        <div>
          <IconSVG name="icon-cog-empty" />
          <div>Actions</div>
        </div>
      );
    };

    return (
      <div className="detail-view-basic-info">
        <div className="profile-detail-top-panel">
          <div className="profile-name-wrapper">
            <h2 className="profile-name" title={profile.label || profile.name}>
              {profile.label || profile.name}
            </h2>
            {
              profile.label ?
                <CopyableId
                  id={profile.name}
                  placement="right"
                  label="Profile ID"
                  tooltipText="Click to copy to clipboard"
                />
              :
                null
            }
          </div>
          <div className="profile-actions-wrapper">
            <ProfileStatusToggle
              profile={profile}
              namespace={namespace}
              toggleProfileStatusCallback={this.props.toggleProfileStatusCallback}
            />
            {this.renderDivider(isNativeProfile)}
            <ActionsPopover
              target={actionsElem}
              namespace={namespace}
              profile={profile}
              onDeleteClick={this.toggleDeleteModal}
            />
          </div>
        </div>
        <div className="profile-description">
          {profile.description}
        </div>
        {this.renderProfileInfoGrid()}
        {this.renderDeleteConfirmationModal()}
        {
          this.state.redirectToListView ?
            <Redirect to={redirectToObj} />
          :
            null
        }
      </div>
    );
  }
}
