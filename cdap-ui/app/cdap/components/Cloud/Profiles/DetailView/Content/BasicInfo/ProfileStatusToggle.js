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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import ConfirmationModal from 'components/ConfirmationModal';
import ToggleSwitch from 'components/ToggleSwitch';
import Alert from 'components/Alert';
import { PROFILE_STATUSES } from 'components/Cloud/Profiles/Store';
import { extractProfileName } from 'components/Cloud/Profiles/Store/ActionCreator';
import { MyCloudApi } from 'api/cloud';
import T from 'i18n-react';
import { CLOUD, SYSTEM_NAMESPACE } from 'services/global-constants';

const PREFIX = 'features.Cloud.Profiles';

export default class ProfileStatusToggle extends Component {
  state = {
    disableModalOpen: false,
    disableErrMsg: '',
    extendedDisableErrMsg: '',
    disableLoading: false,
  };

  static propTypes = {
    profile: PropTypes.object,
    namespace: PropTypes.string,
    toggleProfileStatusCallback: PropTypes.func,
  };

  toggleDisableModal = () => {
    this.setState({
      disableModalOpen: !this.state.disableModalOpen,
      disableErrMsg: '',
      extendedDisableErrMsg: '',
      disableLoading: false,
    });
  };

  toggleProfileStatus = () => {
    this.setState({
      disableLoading: true,
    });

    const profile = this.props.profile;
    const action = PROFILE_STATUSES[profile.status] === 'enabled' ? 'disable' : 'enable';
    let apiObservable$;
    if (this.props.namespace === SYSTEM_NAMESPACE) {
      apiObservable$ = MyCloudApi.toggleSystemProfileStatus({
        profile: profile.name,
        action,
      });
    } else {
      apiObservable$ = MyCloudApi.toggleProfileStatus({
        namespace: this.props.namespace,
        profile: profile.name,
        action,
      });
    }
    apiObservable$.subscribe(
      () => {
        if (this.state.disableModalOpen) {
          this.toggleDisableModal();
        }
        if (typeof this.props.toggleProfileStatusCallback === 'function') {
          this.props.toggleProfileStatusCallback();
        }
      },
      (err) => {
        this.setState({
          disableErrMsg: T.translate(`${PREFIX}.DetailView.disableError`),
          extendedDisableErrMsg: err.response || err,
          disableLoading: false,
        });
      }
    );
  };

  closeAlertBanner = () => {
    this.setState({
      extendedDisableErrMsg: '',
    });
  };

  toggleFunction = (profileIsEnabled) => {
    if (profileIsEnabled) {
      this.toggleDisableModal();
    } else {
      this.toggleProfileStatus();
    }
  };

  renderDisableConfirmationModal() {
    if (!this.state.disableModalOpen) {
      return null;
    }

    const profile = this.props.profile;
    const confirmationText = T.translate(`${PREFIX}.DetailView.disableConfirmation`, {
      profile: profile.name,
    });

    return (
      <ConfirmationModal
        headerTitle={T.translate(`${PREFIX}.DetailView.disableTitle`)}
        toggleModal={this.toggleDisableModal}
        confirmationText={confirmationText}
        confirmButtonText={T.translate(`${PREFIX}.DetailView.disableYes`)}
        confirmFn={this.toggleProfileStatus}
        cancelFn={this.toggleDisableModal}
        isOpen={this.state.disableModalOpen}
        errorMessage={this.state.disableErrMsg}
        extendedMessage={this.state.extendedDisableErrMsg}
        isLoading={this.state.disableLoading}
      />
    );
  }

  // Since for enabling a profile, we don't show a confirmation modal,
  // so if there's an error while enabling a profile, we show it in an
  // Alert banner at the top of the screen
  renderError() {
    if (!this.state.extendedDisableErrMsg || this.state.disableModalOpen) {
      return null;
    }

    const message = T.translate(`${PREFIX}.DetailView.enableError`, {
      message: this.state.extendedDisableErrMsg,
    });

    return (
      <Alert message={message} type="error" showAlert={true} onClose={this.closeAlertBanner} />
    );
  }

  render() {
    const profile = this.props.profile;
    const isNativeProfile = profile.name === extractProfileName(CLOUD.DEFAULT_PROFILE_NAME);

    if (isNativeProfile) {
      return null;
    }

    const profileStatus = PROFILE_STATUSES[profile.status];
    const profileIsEnabled = profileStatus === 'enabled';

    return (
      <div>
        <ToggleSwitch
          isOn={profileIsEnabled}
          onToggle={this.toggleFunction.bind(this, profileIsEnabled)}
          onLabel={T.translate(`${PREFIX}.common.${profileStatus}`)}
          offLabel={T.translate(`${PREFIX}.common.${profileStatus}`)}
        />
        {this.renderDisableConfirmationModal()}
        {this.renderError()}
      </div>
    );
  }
}
