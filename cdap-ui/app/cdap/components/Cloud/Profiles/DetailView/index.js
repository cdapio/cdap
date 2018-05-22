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
import {MyCloudApi} from 'api/cloud';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import {objectQuery} from 'services/helpers';
import EntityTopPanel from 'components/EntityTopPanel';
import ProfileDetailViewContent from 'components/Cloud/Profiles/DetailView/Content';
import {ADMIN_CONFIG_ACCORDIONS} from 'components/Administration/AdminConfigTabContent';
import {getCurrentNamespace} from 'services/NamespaceStore';

require('./DetailView.scss');

export default class ProfileDetailView extends Component {
  state = {
    profile: {},
    provisioners: [],
    loading: true,
    error: null,
    isSystem: objectQuery(this.props.match, 'params', 'namespace') === 'system'
  };

  static propTypes = {
    location: PropTypes.object,
    match: PropTypes.object
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

  getProfile() {
    let {namespace, profileId} = this.props.match.params;
    MyCloudApi
      .get({
        namespace,
        profile: profileId
      })
      .subscribe(
        (profile) => {
          this.setState({
            profile,
            loading: false
          });
        },
        (error) => {
          this.setState({
            error: error.response || error,
            loading: false
          });
        }
      );
  }

  getProvisioners() {
    MyCloudApi
      .getProvisioners()
      .subscribe(
        (provisioners) => {
          this.setState({
            provisioners
          });
        },
        (error) => {
          this.setState({
            error: error.response || error
          });
        }
      );
  }

  render() {
    if (this.state.loading) {
      return <LoadingSVGCentered />;
    }

    let closeBtnlinkObj = this.state.isSystem ? {
      pathname: '/administration/configuration',
      state: { accordionToExpand: ADMIN_CONFIG_ACCORDIONS.systemProfiles }
    } : () => history.back();
    let breadCrumbLabel = this.state.isSystem ? 'Administration' : 'Namespace';
    let breadCrumbAnchorLink = this.state.isSystem ? {
      pathname: '/administration/configuration',
      state: { accordionToExpand: ADMIN_CONFIG_ACCORDIONS.systemProfiles }
    } : `/ns/${getCurrentNamespace()}/details`;
    return (
      <div className="profile-detail-view">
        <EntityTopPanel
          breadCrumbAnchorLink={breadCrumbAnchorLink}
          breadCrumbAnchorLabel={breadCrumbLabel}
          title="Compute Profile Overview"
          closeBtnAnchorLink={closeBtnlinkObj}
        />
        {
          this.state.error ?
            (
              <div className="text-danger text-xs-center">
                {this.state.error}
              </div>
            )
          :
            <ProfileDetailViewContent
              profile={this.state.profile}
              provisioners={this.state.provisioners}
              isSystem={this.state.isSystem}
            />
        }
      </div>
    );
  }
}
