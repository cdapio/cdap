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
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import { objectQuery } from 'services/helpers';
import EntityTopPanel from 'components/EntityTopPanel';
import ProfileDetailViewContent from 'components/Cloud/Profiles/DetailView/Content';
import { ADMIN_CONFIG_ACCORDIONS } from 'components/Administration/AdminConfigTabContent';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { getProvisionersMap } from 'components/Cloud/Profiles/Store/Provisioners';
import {
  ONEDAYMETRICKEY,
  OVERALLMETRICKEY,
  fetchAggregateProfileMetrics,
} from 'components/Cloud/Profiles/Store/ActionCreator';
import Helmet from 'react-helmet';
import T from 'i18n-react';
import { SYSTEM_NAMESPACE } from 'services/global-constants';
import { Theme } from 'services/ThemeHelper';

const PREFIX = 'features.Cloud.Profiles.DetailView';
require('./DetailView.scss');

export default class ProfileDetailView extends Component {
  state = {
    profile: {},
    [ONEDAYMETRICKEY]: {
      runs: '--',
      minutes: '--',
    },
    [OVERALLMETRICKEY]: {
      runs: '--',
      minutes: '--',
    },
    provisioners: [],
    loading: true,
    error: null,
    isSystem: objectQuery(this.props.match, 'params', 'namespace') === SYSTEM_NAMESPACE,
  };

  static propTypes = {
    location: PropTypes.object,
    match: PropTypes.object,
  };

  componentDidMount() {
    this.getProfile();
    this.getProvisioners();

    if (this.state.isSystem) {
      document.querySelector('#header-namespace-dropdown').style.display = 'none';
    }
  }

  componentWillUnmount() {
    document.querySelector('#header-namespace-dropdown').style.display = 'inline-block';
  }

  fetchAggregateMetrics = () => {
    let { namespace } = this.props.match.params;
    let { profile } = this.state;
    let extraTags = {
      profile: `${profile.name}`,
      programtype: 'Workflow',
    };
    fetchAggregateProfileMetrics(namespace, profile, extraTags).subscribe((metricsMap) => {
      this.setState({
        ...metricsMap,
      });
    });
  };

  fetchMetrics = () => {
    this.fetchAggregateMetrics();
  };

  getProfile = () => {
    let { namespace, profileId } = this.props.match.params;
    let apiObservable$;
    if (namespace === SYSTEM_NAMESPACE) {
      apiObservable$ = MyCloudApi.getSystemProfile({ profile: profileId });
    } else {
      apiObservable$ = MyCloudApi.get({ namespace, profile: profileId });
    }
    apiObservable$.subscribe(
      (profile) => {
        this.setState(
          {
            profile,
            loading: false,
          },
          this.fetchMetrics
        );
      },
      (error) => {
        this.setState({
          error: error.response || error,
          loading: false,
        });
      }
    );
  };

  getProvisioners() {
    getProvisionersMap().subscribe((state) => {
      this.setState({
        provisioners: state.list,
      });
    });
  }

  render() {
    if (this.state.loading) {
      return <LoadingSVGCentered />;
    }

    let closeBtnlinkObj = this.state.isSystem
      ? {
          pathname: '/administration/configuration',
          state: { accordionToExpand: ADMIN_CONFIG_ACCORDIONS.systemProfiles },
        }
      : () => history.back();
    let breadCrumbLabel = this.state.isSystem ? 'Administration' : 'Namespace';
    let breadCrumbAnchorLink = this.state.isSystem
      ? {
          pathname: '/administration/configuration',
          state: { accordionToExpand: ADMIN_CONFIG_ACCORDIONS.systemProfiles },
        }
      : `/ns/${getCurrentNamespace()}/details`;
    let { namespace } = this.props.match.params;
    if (!namespace) {
      namespace = getCurrentNamespace();
    }
    return (
      <div className="profile-detail-view">
        <Helmet
          title={T.translate(`${PREFIX}.pageTitle`, {
            profile_name: this.state.profile.label || this.state.profile.name,
            productName: Theme.productName,
          })}
        />
        <EntityTopPanel
          breadCrumbAnchorLink={breadCrumbAnchorLink}
          breadCrumbAnchorLabel={breadCrumbLabel}
          title={T.translate(`${PREFIX}.computeProfileOverview`)}
          closeBtnAnchorLink={closeBtnlinkObj}
        />
        {this.state.error ? (
          <div className="text-danger text-center">{this.state.error}</div>
        ) : (
          <ProfileDetailViewContent
            profile={this.state.profile}
            provisioners={this.state.provisioners}
            isSystem={this.state.isSystem}
            toggleProfileStatusCallback={this.getProfile}
            namespace={namespace}
            oneDayMetrics={this.state[ONEDAYMETRICKEY]}
            overallMetrics={this.state[OVERALLMETRICKEY]}
          />
        )}
      </div>
    );
  }
}
