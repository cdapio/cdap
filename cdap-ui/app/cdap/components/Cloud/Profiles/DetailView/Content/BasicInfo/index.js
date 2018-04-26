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
import {MyCloudApi} from 'api/cloud';
import {Redirect} from 'react-router-dom';
import {ADMIN_CONFIG_ACCORDIONS} from 'components/Administration/AdminConfigTabContent';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import ActionsPopover from 'components/Cloud/Profiles/ActionsPopover';

require('./BasicInfo.scss');

const PREFIX = 'features.Cloud.Profiles';

export default class ProfileDetailViewBasicInfo extends Component {
  state = {
    deleteModalOpen: false,
    deleteErrMsg: '',
    extendedDeleteErrMsg: '',
    deleteLoading: false,
    redirectToListView: false,
  };

  static propTypes = {
    profile: PropTypes.object,
    isSystem: PropTypes.bool
  };

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

    MyCloudApi
      .delete({
        namespace: this.props.isSystem ? 'system' : getCurrentNamespace(),
        profile: this.props.profile.name
      })
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

    return (
      <ConfirmationModal
        headerTitle={T.translate(`${PREFIX}.common.deleteTitle`)}
        toggleModal={this.toggleDeleteModal}
        confirmationText={confirmationText}
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
                <span>{profile.provisioner.name}</span>
              </div>
              <div>{profile.scope}</div>
              <div />
              <div />
              <div />
              <div />
              <div />
            </div>
          </div>
        </div>
      </div>
    );
  }

  render() {
    let profile = this.props.profile;
    let redirectToObj = this.props.isSystem ? {
      pathname: '/administration/configuration',
      state: { accordionToExpand: ADMIN_CONFIG_ACCORDIONS.systemProfiles }
    } : `/ns/${getCurrentNamespace()}/details`;
    let namespace = this.props.isSystem ? 'system' : getCurrentNamespace();

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
        <div className="profile-name-delete">
          <h2 className="profile-name">
            {profile.name}
          </h2>
          <ActionsPopover
            target={actionsElem}
            namespace={namespace}
            profile={profile}
            onDeleteClick={this.toggleDeleteModal}
          />
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
