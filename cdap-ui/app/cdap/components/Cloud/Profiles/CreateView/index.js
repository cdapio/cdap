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
import {Form, FormGroup, Col} from 'reactstrap';
import AbstractWidget from 'components/AbstractWidget';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {Link, Redirect} from 'react-router-dom';
import {MyCloudApi} from 'api/cloud';
import {objectQuery, preventPropagation} from 'services/helpers';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import {connect, Provider} from 'react-redux';
import ProvisionerInfoStore from 'components/Cloud/Store';
import {fetchProvisionerSpec} from 'components/Cloud/Store/ActionCreator';
import {ADMIN_CONFIG_ACCORDIONS} from 'components/Administration/AdminConfigTabContent';
import EntityTopPanel from 'components/EntityTopPanel';
import PropertyLock from 'components/Cloud/Profiles/CreateView/PropertyLock';
import { UncontrolledTooltip } from 'components/UncontrolledComponents';
import {ConnectedProfileName, ConnectedProfileDescription} from 'components/Cloud/Profiles/CreateView/CreateProfileMetadata';
import {
  initializeProperties,
  updateProperty,
  resetCreateProfileStore
} from 'components/Cloud/Profiles/CreateView/CreateProfileActionCreator';
import CreateProfileBtn from 'components/Cloud/Profiles/CreateView/CreateProfileBtn';
import uuidV4 from 'uuid/v4';
import CreateProfileStore from 'components/Cloud/Profiles/CreateView/CreateProfileStore';

require('./CreateView.scss');

class ProfileCreateView extends Component {
  static propTypes = {
    match: PropTypes.object,
    provisionerJsonSpecMap: PropTypes.object,
    loading: PropTypes.bool
  };

  static defaultProps = {
    provisionerJsonSpecMap: {}
  };

  state = {
    redirectToNamespace: false,
    redirectToAdmin: false,
    creatingProfile: false,
    isSystem: objectQuery(this.props.match, 'params', 'namespace') === 'system',
    selectedProvisioner: objectQuery(this.props.match, 'params', 'provisionerId')
  };

  componentWillReceiveProps(nextProps) {
    let {selectedProvisioner} = this.state;
    initializeProperties(nextProps.provisionerJsonSpecMap[selectedProvisioner]);
  }

  componentDidMount() {
    let {selectedProvisioner} = this.state;
    fetchProvisionerSpec(selectedProvisioner);
    // FIXME: Since we are already in admin we shouldn't have to do this explicitly from the create profile view.
    if (this.state.isSystem) {
      document.querySelector('#header-namespace-dropdown').style.display = 'none';
    }
  }

  componentWillUnmount() {
    document.querySelector('#header-namespace-dropdown').style.display = 'inline-block';
    resetCreateProfileStore();
  }

  createProfile = () => {
    this.setState({
      creatingProfile: true
    });
    let {name, description, properties} = CreateProfileStore.getState();
    let jsonBody = {
      description,
      provisioner: {
        name: this.state.selectedProvisioner,
        properties: Object.entries(properties).map(([property, propObj]) => {
          return {
            name: property,
            value: propObj.value,
            isEditable: propObj.isEditable
          };
        })
      }
    };
    MyCloudApi
      .create({
        namespace: this.state.isSystem ? 'system' : getCurrentNamespace(),
        profile: name
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
          <span className="required-marker text-danger">*</span>
        </Col>
        <Col xs="5">
          <ConnectedProfileName />
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
          <span className="required-marker text-danger">*</span>
        </Col>
        <Col xs="5">
          <ConnectedProfileDescription />
        </Col>
      </FormGroup>
    );
  };

  renderGroup = (group) => {
    let {properties} = CreateProfileStore.getState();
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
              let uniqueId = `provisioner-${uuidV4()}`;
              return (
                <FormGroup key={uniqueId} row>
                  <Col xs="3">
                    <strong
                      className="label"
                      id={uniqueId}
                    >
                      {property.label}
                    </strong>
                    {
                      property.required ?
                        <span className="required-marker text-danger">*</span>
                      :
                        null
                    }
                    {
                      property.description ?
                        <UncontrolledTooltip
                          placement="right"
                          delay={0}
                          target={uniqueId}
                          className="provisioner-tooltip"
                        >
                          {property.description}
                        </UncontrolledTooltip>
                      :
                        null
                    }
                  </Col>
                  <Col xs="5">
                    {
                      <AbstractWidget
                        type={property['widget-type']}
                        value={objectQuery(properties, property.name, 'value')}
                        onChange={updateProperty.bind(null, property.name)}
                        widgetProps={property['widget-attributes']}
                      />
                    }
                    <PropertyLock propertyName={property.name} />
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
    let {selectedProvisioner} = this.state;
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
    } : () => history.back();

    return (
      <Provider store={CreateProfileStore}>
        <div className="profile-create-view">
          <EntityTopPanel
            title="Create a Google Dataproc Profile"
            closeBtnAnchorLink={linkObj}
          />
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
                {this.renderGroups()}
                {
                  this.props.loading ?
                    <LoadingSVGCentered />
                  :
                    null
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
            <CreateProfileBtn
              className="btn-primary"
              onClick={this.createProfile}
              loading={this.state.creatingProfile}
            />
            {
              typeof linkObj === 'function' ?
                <button className="btn btn-link" onClick={linkObj}> Close </button>
              :
                <Link to={linkObj}>
                  Close
                </Link>
            }
          </div>
        </div>
      </Provider>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    loading: state.loading,
    provisionerJsonSpecMap: state.map
  };
};
const ConnectedProfileCreateView = connect(mapStateToProps)(ProfileCreateView);

export default function ProfileCreateViewFn({...props}) {
  return (
    <Provider store={ProvisionerInfoStore}>
      <ConnectedProfileCreateView {...props} />
    </Provider>
  );
}
