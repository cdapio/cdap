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
import {Form, FormGroup, Col, Input, Button} from 'reactstrap';
import {preventPropagation} from 'services/helpers';
import SampleViewSpecJson from './sample-view-spec.json';
import AbstractWidget from 'components/AbstractWidget';
import {getCurrentNamespace} from 'services/NamespaceStore';
import {Link} from 'react-router-dom';

require('./CreateView.scss');

export default class ProfilesCreateView extends Component {

  state = {
    profileName: '',
    profileDescription: ''
  };

  parseSpecAndGetInitialState = () => {
    let configs = SampleViewSpecJson['configuration-groups'];
    let properties = {};
    configs.forEach(config => {
      config.properties.forEach(prop => {
        properties[prop.name] = {
          value: prop['widget-attributes'].default,
          editable: true
        };
      });
    });
    return properties;
  };

  properties = this.parseSpecAndGetInitialState();

  componentDidMount() {
    if (this.profileNameInput) {
      this.profileNameInput.focus();
    }
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
    let jsonBody = {
      description: this.state.profileDescription,
      provisioner: {
        name: 'GoogleDataProc',
        properties: Object.entries(this.properties).map(([property, propObj]) => {
          return {
            name: property,
            value: propObj.value,
            editable: propObj.editable
          };
        })
      }
    };
    console.log(`Coming soon. This is the state right now:
      ${JSON.stringify(jsonBody, null, 2)}
    `);
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
      <div className="group-container" key={group.name}>
        <strong className="group-title"> {group.name} </strong>
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

  render() {
    let configurationGroups = SampleViewSpecJson['configuration-groups'];
    return (
      <div className="profile-create-view">
        <div className="create-view-top-panel">
          Create a Goolge Dataproc Profile
        </div>
        <div className="create-form-container">
          <Form
            className="form-horizontal"
            onSubmit={(e) => {
              preventPropagation(e);
              return false;
            }}
          >
            {this.renderProfileName()}
            {this.renderDescription()}
            {
              configurationGroups.map(group => this.renderGroup(group))
            }
          </Form>
        </div>
        <div className="btns-section">
          <Button
            color="primary"
            onClick={this.createProfile}
            disabled={!this.state.profileName.length || !this.state.profileDescription.length}
          >
            Create Compute Profile
          </Button>
          <Link to={`/ns/${getCurrentNamespace()}`}>
            Close
          </Link>
        </div>
      </div>
    );
  }
}
