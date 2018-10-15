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
import { MyCloudApi } from 'api/cloud';
import { getCurrentNamespace } from 'services/NamespaceStore';
import LoadingSVG from 'components/LoadingSVG';
import {
  getProvisionerLabel,
  getNodeHours,
  fetchAggregateProfileMetrics,
  ONEDAYMETRICKEY,
} from 'components/Cloud/Profiles/Store/ActionCreator';
import { PROFILE_STATUSES } from 'components/Cloud/Profiles/Store';
import { humanReadableDate } from 'services/helpers';
import T from 'i18n-react';
import { SCOPES, SYSTEM_NAMESPACE } from 'services/global-constants';
require('./Preview.scss');

export default class ProfilePreview extends Component {
  static propTypes = {
    profileScope: PropTypes.string,
    profileLabel: PropTypes.string,
    profileName: PropTypes.string,
    profileCustomProperties: PropTypes.object,
  };
  state = {
    profileDetails: null,
    loading: true,
    error: null,
    metrics: {
      runs: '--',
      minutes: '--',
    },
    provisioners: [],
  };

  componentDidMount() {
    this.getProfileDetails();
    this.getProvisioners();
  }

  getProfileDetails() {
    let namespace = getCurrentNamespace();
    let apiObservable$;
    if (this.props.profileScope === SCOPES.SYSTEM) {
      apiObservable$ = MyCloudApi.getSystemProfile({ profile: this.props.profileName });
    } else {
      apiObservable$ = MyCloudApi.get({
        namespace,
        profile: this.props.profileName,
      });
    }
    apiObservable$.subscribe(
      (profileDetails) => {
        this.setState(
          {
            profileDetails,
            loading: false,
          },
          this.getProfileMetrics
        );
      },
      (error) => {
        this.setState({
          error,
          loading: false,
        });
      }
    );
  }

  getProfileMetrics = () => {
    let { profileName } = this.props;
    let { profileDetails } = this.state;
    const extraTags = {
      profilescope: profileDetails.scope,
      programtype: 'Workflow',
      profile: `${profileName}`,
    };
    fetchAggregateProfileMetrics(getCurrentNamespace(), profileDetails, extraTags).subscribe(
      (metricsMap) => {
        let oneDayMetrics = metricsMap[ONEDAYMETRICKEY];
        this.setState({
          metrics: oneDayMetrics,
        });
      }
    );
  };
  getProvisioners() {
    MyCloudApi.getProvisioners().subscribe(
      (provisioners) => {
        this.setState({
          provisioners,
        });
      },
      (error) => {
        this.setState({
          error: error.response || error,
        });
      }
    );
  }

  render() {
    if (this.state.loading) {
      return (
        <div className="profile-preview text-center">
          <LoadingSVG />
        </div>
      );
    }
    let profileNamespace =
      this.state.profileDetails.scope === SCOPES.SYSTEM ? SYSTEM_NAMESPACE : getCurrentNamespace();
    let profileDetailsLink = `${location.protocol}//${
      location.host
    }/cdap/ns/${profileNamespace}/profiles/details/${this.props.profileName}`;
    let profileProvisionerLabel = getProvisionerLabel(
      this.state.profileDetails,
      this.state.provisioners
    );
    const profileStatus = PROFILE_STATUSES[this.state.profileDetails.status];
    const profileLabel = this.props.profileLabel || this.props.profileName;

    return (
      <div className="profile-preview text-left">
        <div className="truncate">
          <strong title={profileLabel}>{profileLabel}</strong>
        </div>
        <div className="profile-descripion">
          <p className="multi-line-text">{this.state.profileDetails.description}</p>
        </div>
        <div className="grid grid-container">
          <div className="grid-header">
            <div className="grid-row sub-header">
              <div />
              <div />
              <div className="sub-title">Last 24 hours</div>
              <div />
            </div>
            <div className="grid-row">
              <div>Provisioner</div>
              <div>Scope</div>
              <div>Runs</div>
              <div>Node hours</div>
              <div>Created</div>
              <div>Status</div>
            </div>
          </div>
          <div className="grid-body">
            <div className="grid-row">
              <div>
                <span className="provisioner-name truncate-text">{profileProvisionerLabel}</span>
              </div>
              <div className="truncate-text">{this.state.profileDetails.scope}</div>
              <div>{this.state.metrics.runs || '--'}</div>
              <div>{getNodeHours(this.state.metrics.minutes || '--')}</div>
              <div>{humanReadableDate(this.state.profileDetails.created, false, true)}</div>
              <div className={`profile-status ${profileStatus}`}>
                {T.translate(`features.Cloud.Profiles.common.${profileStatus}`)}
              </div>
            </div>
          </div>
        </div>
        <hr />
        <div>
          <a href={profileDetailsLink}>View Details</a>
        </div>
      </div>
    );
  }
}
