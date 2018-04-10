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
import {Form, FormGroup, Col, Input} from 'reactstrap';
import AbstractWidget from 'components/AbstractWidget';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {Link, Redirect} from 'react-router-dom';
import {MyCloudApi} from 'api/cloud';
import BtnWithLoading from 'components/BtnWithLoading';
import {objectQuery, preventPropagation} from 'services/helpers';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import {connect, Provider} from 'react-redux';
import ProvisionerInfoStore from 'components/Cloud/Store';
import {fetchProvisionerSpec} from 'components/Cloud/Store/ActionCreator';
import {ADMIN_CONFIG_ACCORDIONS} from 'components/Administration/AdminConfigTabContent';
import IconSVG from 'components/IconSVG';

require('./CreateView.scss');

class ProfilesCreateView extends Component {
  static propTypes = {
    location: PropTypes.object,
    provisionerJsonSpecMap: PropTypes.object,
    loading: PropTypes.bool,
    selectedProvisioner: PropTypes.string
  };

  static defaultProps = {
    provisionerJsonSpecMap: {}
  };

  state = {
    profileName: '',
    profileDescription: '',
    redirectToNamespace: false,
    redirectToAdmin: false,
    creatingProfile: false,
    isSystem: objectQuery(this.props.location, 'pathname') === '/create-profile'
  };

  parseSpecAndGetInitialState = (provisionerJson = {}) => {
    let configs = provisionerJson['configuration-groups'] || [];
    let properties = {};
    configs.forEach(config => {
      config.properties.forEach(prop => {
        let widgetAttributes = prop['widget-attributes'] || {};
        properties[prop.name] = {
          value: widgetAttributes.default,
          editable: true
        };
      });
    });
    return properties;
  };

  componentWillReceiveProps(nextProps) {
    let {selectedProvisioner} = nextProps;
    this.properties = this.parseSpecAndGetInitialState(nextProps.provisionerJsonSpecMap[selectedProvisioner]);
  }

  componentDidMount() {
    let {selectedProvisioner} = this.props;
    // The name will probably come from the URL in the future when we add new provisioner types.
    fetchProvisionerSpec(selectedProvisioner);
    if (this.profileNameInput) {
      this.profileNameInput.focus();
    }
    if (this.state.isSystem) {
      document.querySelector('#header-namespace-dropdown').style.display = 'none';
    }
  }

  componentWillUnmount() {
    document.querySelector('#header-namespace-dropdown').style.display = 'inline-block';
  }

  onValueChange = (property, value) => {
    this.properties[property].value = value;
  };

  onMetadataChange = (metadata, e) => {
    this.setState({
      [metadata]: e.target.value
    });
  };

  createProfile = () => {
    this.setState({
      creatingProfile: true
    });
    let jsonBody = {
      description: this.state.profileDescription,
      provisioner: {
        name: this.props.selectedProvisioner,
        properties: Object.entries(this.properties).map(([property, propObj]) => {
          return {
            name: property,
            value: propObj.value,
            editable: propObj.editable
          };
        })
      }
    };
    MyCloudApi
      .create({
        namespace: this.state.isSystem ? 'system' : getCurrentNamespace(),
        profile: this.state.profileName
      }, jsonBody)
      .subscribe(
        () => {
          if (this.state.isSystem) {
            this.setState({
              redirectToAdmin: true
            });
          } else {
            this.setState({
              redirectToNamespace: true
            });
          }
        },
        err => {
          this.setState({
            creatingProfile: false,
            error: err.response
          });
        }
      );
  };

  renderProfileName = () => {
    return (
      <FormGroup row>
        <Col xs="3">
          <strong
            className="label"
            id="profile-name"
          >
            Profile Name
          </strong>
        </Col>
        <Col xs="5">
          <Input
            aria-labelledby="profile-name"
            getRef={ref => this.profileNameInput = ref}
            value={this.state.profileName}
            onChange={this.onMetadataChange.bind(this, 'profileName')}
            placeholder="Add a name for the Compute Profile"
          />
        </Col>
      </FormGroup>
    );
  };

  renderDescription = () => {
    return (
      <FormGroup row>
        <Col xs="3">
          <strong
            className="label"
            id="profile-description"
          >
            Description
          </strong>
        </Col>
        <Col xs="5">
          <Input
            type="textarea"
            aria-labelledby="profile-description"
            value={this.state.profileDescription}
            onChange={this.onMetadataChange.bind(this, 'profileDescription')}
            placeholder="Add a description for the profile"
          />
        </Col>
      </FormGroup>
    );
  };

  renderGroup = (group) => {
    return (
      <div className="group-container" key={group.label}>
        <strong className="group-title"> {group.label} </strong>
        <hr />
        <div className="group-description">
          {group.description}
        </div>
        <div className="fields-container">
          {
            group.properties.map(property => {
              let uniqueId = `${group.name}-${property.name}`;
              return (
                <FormGroup key={uniqueId} row>
                  <Col xs="3">
                    <strong
                      className="label"
                      id={uniqueId}
                    >
                      {property.label}
                    </strong>
                  </Col>
                  <Col xs="5">
                    {
                      <AbstractWidget
                        type={property['widget-type']}
                        value=""
                        onChange={this.onValueChange.bind(this, property.name)}
                        widgetProps={property['widget-attributes']}
                      />
                    }
                  </Col>
                </FormGroup>
              );
            })
          }
        </div>
      </div>
    );
  };

  renderGroups = () => {
    let {selectedProvisioner} = this.props;
    let configurationGroups = objectQuery(this.props, 'provisionerJsonSpecMap', selectedProvisioner, 'configuration-groups');
    if (!configurationGroups) {
      return null;
    }
    return configurationGroups.map(group => this.renderGroup(group));
  };

  render() {
    if (this.state.redirectToNamespace) {
      return (
        <Redirect to={`/ns/${getCurrentNamespace()}/details`} />
      );
    }
    if (this.state.redirectToAdmin) {
      return (
        <Redirect to={{
          pathname: '/administration/configuration',
          state: { accordionToExpand: ADMIN_CONFIG_ACCORDIONS.systemProfiles }
        }}/>
      );
    }

    let linkObj = this.state.isSystem ? {
      pathname: '/administration/configuration',
      state: { accordionToExpand: ADMIN_CONFIG_ACCORDIONS.systemProfiles }
    } : `/ns/${getCurrentNamespace()}/details`;

    return (
      <div className="profile-create-view">
        <div className="create-view-top-panel">
          <span>
            Create a Google Dataproc Profile
          </span>
          <Link
            className="close-create-view"
            to={linkObj}
          >
            <IconSVG name="icon-close" />
          </Link>
        </div>
        <div className="create-form-container">
          <fieldset disabled={this.state.creatingProfile}>
            <Form
              className="form-horizontal"
              onSubmit={(e) => {
                preventPropagation(e);
                return false;
              }}
            >
              <div className="group-container">
                {this.renderProfileName()}
                {this.renderDescription()}
              </div>
              {
                this.props.loading ?
                  <LoadingSVGCentered />
                :
                  this.renderGroups()
              }
            </Form>
          </fieldset>
        </div>
        {
          this.state.error ?
            <div className="error-section text-danger">
              {this.state.error}
            </div>
          :
            null
        }
        <div className="btns-section">
          <BtnWithLoading
            className="btn-primary"
            onClick={this.createProfile}
            loading={this.state.creatingProfile}
            disabled={!this.state.profileName.length || !this.state.profileDescription.length}
            label="Create Compute Profile"
          />
          <Link to={linkObj}>
            Close
          </Link>
        </div>
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    loading: state.loading,
    provisionerJsonSpecMap: state.map,
    selectedProvisioner: state.selectedProvisioner
  };
};
const ConnectedProfilesCreateView = connect(mapStateToProps)(ProfilesCreateView);

export default function ProfilesCreateViewFn({...props}) {
  return (
    <Provider store={ProvisionerInfoStore}>
      <ConnectedProfilesCreateView {...props} />
    </Provider>
  );
}
