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
import {Link} from 'react-router-dom';
import {objectQuery} from 'services/helpers';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import {connect, Provider} from 'react-redux';
import ProvisionerInfoStore from 'components/Cloud/Store';
import {fetchProvisioners} from 'components/Cloud/Store/ActionCreator';
import {ADMIN_CONFIG_ACCORDIONS} from 'components/Administration/AdminConfigTabContent';
import EntityTopPanel from 'components/EntityTopPanel';

require('./ProvisionerSelection.scss');

class ProfileCreateProvisionerSelection extends Component {
  static propTypes = {
    match: PropTypes.object,
    provisionerJsonSpecMap: PropTypes.object,
    loading: PropTypes.bool,
    error: PropTypes.any
  };

  static defaultProps = {
    provisionerJsonSpecMap: {}
  };

  state = {
    isSystem: objectQuery(this.props.match, 'params', 'namespace') === 'system'
  };

  componentDidMount() {
    fetchProvisioners();
    if (this.state.isSystem) {
      document.querySelector('#header-namespace-dropdown').style.display = 'none';
    }
  }

  componentWillUnmount() {
    document.querySelector('#header-namespace-dropdown').style.display = 'inline-block';
  }

  renderProvisionerBox(provisioner) {
    let namespace = this.props.match.params.namespace;

    return (
      <Link
        to={`/ns/${namespace}/profiles/create/${provisioner.name}`}
        className="provisioner-box"
      >
        <div className="provisioner-content">
          <div className="provisioner-icon">
            {/* TODO: Fetch image from backend when it's available */}
            <img src="/cdap_assets/img/GCDataProc.png" />
          </div>
          <div className="provisioner-label">
            {provisioner.label || provisioner.name}
          </div>
          <div className="provisioner-description">
            {provisioner.description}
          </div>
        </div>
      </Link>
    );
  }

  render() {
    let linkObj = this.state.isSystem ? {
      pathname: '/administration/configuration',
      state: { accordionToExpand: ADMIN_CONFIG_ACCORDIONS.systemProfiles }
    } : () => history.back();
    let breadCrumbLabel = this.state.isSystem ? 'Administration' : 'Namespace';
    let breadCrumbAnchorLink = this.state.isSystem ? {
      pathname: '/administration/configuration',
      state: { accordionToExpand: ADMIN_CONFIG_ACCORDIONS.systemProfiles }
    } : `/ns/${getCurrentNamespace()}/details`;

    return (
      <div className="profile-create-provisioner-selection">
        <EntityTopPanel
          breadCrumbAnchorLink={breadCrumbAnchorLink}
          breadCrumbAnchorLabel={breadCrumbLabel}
          title="Create a Compute Profile"
          closeBtnAnchorLink={linkObj}
        />
        <div className="provisioner-selection-container">
          <h3 className="selection-container-label">
            Select a Provisioner for your Compute Profile
          </h3>
          <div className="provisioner-selections">
            {
              this.props.loading ?
                <LoadingSVGCentered />
              :
                Object.values(this.props.provisionerJsonSpecMap)
                  .filter(provisioner => provisioner.name !== 'yarn')
                  .map(provisioner => {
                    return this.renderProvisionerBox(provisioner);
                  })
            }
          </div>
        </div>
        {
          this.props.error ?
            <div className="error-section text-danger">
              {this.props.error}
            </div>
          :
            null
        }
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    loading: state.loading,
    error: state.error,
    provisionerJsonSpecMap: state.map
  };
};
const ConnectedProfileCreateProvisionerSelection = connect(mapStateToProps)(ProfileCreateProvisionerSelection);

export default function ProfileCreateProvisionerSelectionFn({...props}) {
  return (
    <Provider store={ProvisionerInfoStore}>
      <ConnectedProfileCreateProvisionerSelection {...props} />
    </Provider>
  );
}
