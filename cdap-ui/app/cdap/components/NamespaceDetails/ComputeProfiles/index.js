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
import T from 'i18n-react';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {Link} from 'react-router-dom';
import ProfilesListView from 'components/Cloud/Profiles/ListView';
import ProfilesStore from 'components/Cloud/Profiles/Store';
import {importProfile} from 'components/Cloud/Profiles/Store/ActionCreator';
import {connect, Provider} from 'react-redux';
import {Label, Input} from 'reactstrap';

require('./ComputeProfiles.scss');

const PREFIX = 'features.NamespaceDetails.computeProfiles';

class NamespaceDetailsComputeProfiles extends Component {
  static propTypes = {
    profilesCount: PropTypes.number
  }

  renderProfilesLabel() {
    let label;
    if (!this.props.profilesCount) {
      label = <strong>{T.translate(`${PREFIX}.label`)}</strong>;
    } else {
      label = <strong>{T.translate(`${PREFIX}.labelWithCount`, {count: this.props.profilesCount})}</strong>;
    }

    return (
      <div className="namespace-details-section-label">
        {label}
        <Link
          to={`/ns/${getCurrentNamespace()}/profiles/create`}
          className="create-new-profile-label"
        >
          {T.translate(`${PREFIX}.create`)}
        </Link>
        <span> | </span>
        <Label
          className="import-profile-label"
          for="import-profile"
        >
          {T.translate(`${PREFIX}.import`)}
          {/* The onClick here is to clear the file, so if the user uploads the same file
          twice then we can show the error, instead of showing nothing */}
          <Input
            type="file"
            accept='.json'
            id="import-profile"
            onChange={importProfile.bind(this, getCurrentNamespace())}
            onClick={(e) => e.target.value = null}
          />
        </Label>
        {
          this.props.profilesCount ?
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
        <ProfilesListView
          namespace={getCurrentNamespace()}
        />
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    profilesCount: state.profiles.length
  };
};

const ConnectedComputeProfilesSection = connect(mapStateToProps)(NamespaceDetailsComputeProfiles);

export default function NamespaceDetailsComputeProfilesFn() {
  return (
    <Provider store={ProfilesStore}>
      <ConnectedComputeProfilesSection />
    </Provider>
  );
}
