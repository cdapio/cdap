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
import { connect, Provider } from 'react-redux';
import PropTypes from 'prop-types';
import { objectQuery, preventPropagation, isNilOrEmpty } from 'services/helpers';
import IconSVG from 'components/IconSVG';
import ProfilePreview from 'components/Cloud/Profiles/Preview';
import Popover from 'components/Popover';
import classnames from 'classnames';
import { extractProfileName, getProfiles } from 'components/Cloud/Profiles/Store/ActionCreator';
import ProfilesStore from 'components/Cloud/Profiles/Store';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { CLOUD } from 'services/global-constants';
import T from 'i18n-react';
import isObject from 'lodash/isObject';
import { SCOPES, SYSTEM_NAMESPACE } from 'services/global-constants';

const PREFIX = 'features.PipelineDetails.RunLevel.RunComputeProfile';

require('./RunComputeProfile.scss');

class RunLevelComputeProfile extends Component {
  static propTypes = {
    profileName: PropTypes.string,
    profiles: PropTypes.array,
    isProfileFetchInTrasit: PropTypes.bool,
  };

  state = {
    profilesNameToLabelMap: {},
  };

  componentDidMount() {
    this.fetchProfilesForLabel();
  }

  componentWillReceiveProps(nextProps) {
    this.fetchProfilesForLabel(nextProps);
  }

  fetchProfilesForLabel = (props = this.props) => {
    let { profiles, isProfileFetchInTrasit, profileName } = props;
    if (!isNilOrEmpty(profileName) && !isProfileFetchInTrasit && !profiles.length) {
      return getProfiles(getCurrentNamespace());
    }
    this.updateProfilesMap(props);
  };

  updateProfilesMap = ({ profiles }) => {
    let profilesNameToLabelMap = {};
    profiles.forEach(
      (profile) => (profilesNameToLabelMap[profile.name] = profile.label || profile.name)
    );
    this.setState({
      profilesNameToLabelMap,
    });
  };

  getProfileLabel = () => {
    let profileName = extractProfileName(this.props.profileName);
    let profileLabel = this.state.profilesNameToLabelMap[profileName];
    return isNilOrEmpty(profileLabel) ? profileName : profileLabel;
  };

  render() {
    const ProfileLabel = () => {
      return (
        <div
          className={classnames('profile-preview-label', {
            disabled: this.props.profileName === CLOUD.DEFAULT_PROFILE_NAME,
          })}
        >
          {!this.props.profileName ? (
            <button className="btn btn-link" title={T.translate(`${PREFIX}.noInfo`)} disabled>
              <IconSVG name="icon-cloud" />
              <span>--</span>
            </button>
          ) : (
            <div
              className="profile-preview-label-content truncate"
              onClick={(e) => {
                if (this.props.profileName === CLOUD.DEFAULT_PROFILE_NAME) {
                  preventPropagation(e);
                  return false;
                }
              }}
              title={this.getProfileLabel()}
            >
              <IconSVG name="icon-cloud" />
              <span>{this.getProfileLabel()}</span>
            </div>
          )}
        </div>
      );
    };
    return (
      <div className="run-info-container run-level-compute-profile pull-right">
        <div>
          <strong>{T.translate(`${PREFIX}.label`)}</strong>
        </div>
        {!this.props.profileName ? (
          <ProfileLabel />
        ) : (
          <Popover
            target={() => <ProfileLabel />}
            className="profile-preview-popover"
            placement="bottom-end"
            bubbleEvent={true}
            enableInteractionInPopover={true}
            injectOnToggle={true}
          >
            <ProfilePreview
              profileLabel={this.getProfileLabel()}
              profileName={extractProfileName(this.props.profileName)}
              profileScope={
                this.props.profileName.indexOf(`${SCOPES.SYSTEM}:`) !== -1
                  ? SCOPES.SYSTEM
                  : SCOPES.USER
              }
            />
          </Popover>
        )}
      </div>
    );
  }
}
const mapProfileStateToProps = (state) => {
  return {
    profiles: state.profiles,
    isProfileFetchInTrasit: state.loading,
  };
};

const ProfileConnectedRunLevelComputeProfile = connect(mapProfileStateToProps)(
  RunLevelComputeProfile
);

function ProfileWrappedRunLevelComputeProfile({ ...restProps }) {
  return (
    <Provider store={ProfilesStore}>
      <ProfileConnectedRunLevelComputeProfile {...restProps} />
    </Provider>
  );
}

const getProfileName = (profileObj) => {
  if (isNilOrEmpty(profileObj) || !isObject(profileObj)) {
    return null;
  }
  if (profileObj.namespace === SYSTEM_NAMESPACE) {
    return `SYSTEM:${profileObj.profileName}`;
  }
  return `USER:${profileObj.profileName}`;
};
const mapStateToProps = (state) => {
  return {
    profileName: getProfileName(objectQuery(state, 'currentRun', 'profile')),
  };
};

const ConnectedRunLevelComputeProfile = connect(mapStateToProps)(
  ProfileWrappedRunLevelComputeProfile
);

export default ConnectedRunLevelComputeProfile;
