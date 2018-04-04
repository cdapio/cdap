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
import T from 'i18n-react';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {Link} from 'react-router-dom';
import ProfilesListView from 'components/Cloud/Profiles/ListView';

require('./ComputeProfiles.scss');

const PREFIX = 'features.NamespaceDetails.computeProfiles';

export default class NamespaceDetailsComputeProfiles extends Component {
  state = {
    profilesCount: 0
  };

  onChange = (profiles) => {
    this.setState({
      profilesCount: profiles.length
    });
  }

  renderProfilesLabel() {
    let label;
    if (!this.state.profilesCount) {
      label = <strong>{T.translate(`${PREFIX}.label`)}</strong>;
    } else {
      label = <strong>{T.translate(`${PREFIX}.labelWithCount`, {count: this.state.profilesCount})}</strong>;
    }

    return (
      <div className="namespace-details-section-label">
        {label}
        <Link
          to={`/ns/${getCurrentNamespace()}/create-profile`}
          className="create-new-profile-label"
        >
          {T.translate(`${PREFIX}.create`)}
        </Link>
        {
          this.state.profilesCount ?
            (
              <p className="create-new-profile-description">
                {T.translate(`${PREFIX}.description`)}
              </p>
            )
          :
            null
        }
      </div>
    );
  }

  render() {
    return (
      <div className="namespace-details-compute-profiles">
        {this.renderProfilesLabel()}
        <ProfilesListView onChange={this.onChange} />
      </div>
    );
  }
}
