/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, { Component, PropTypes } from 'react';
import {Col, FormGroup, Label, Form, Input} from 'reactstrap';
import {defaultQueueTypes} from 'services/WizardStores/MicroserviceUpload/MicroserviceQueueStore';
import NamespaceStore from 'services/NamespaceStore';
import SelectWithOptions from 'components/SelectWithOptions';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
require('./MicroserviceQueue.scss');

const PREFIX = 'features.Wizard.MicroserviceUpload.MicroserviceQueue';

export default class MicroserviceQueue extends Component {
  static propTypes = {
    name: PropTypes.string,
    type: PropTypes.string,
    namespace: PropTypes.string,
    topic: PropTypes.string,
    region: PropTypes.string,
    accessKey: PropTypes.string,
    accessId: PropTypes.string,
    queueName: PropTypes.string,
    endpoint: PropTypes.string,
    sslKeystoreFilePath: PropTypes.string,
    sslKeystorePassword: PropTypes.string,
    sslKeystoreKeyPassword: PropTypes.string,
    sslKeystoreType: PropTypes.string,
    sslTruststoreFilePath: PropTypes.string,
    sslTruststorePassword: PropTypes.string,
    sslTruststoreType: PropTypes.string,
    index: PropTypes.number,
    onChange: PropTypes.func,
    onAdd: PropTypes.func,
    onRemove: PropTypes.func
  };

  renderFormGroupProperty(property, required = false, inputType = 'text') {
    return (
      <FormGroup row>
        <Col xs="3">
          <Label className="control-label">{T.translate(`${PREFIX}.labels.${property}`)}</Label>
        </Col>
        <Col xs="8">
          <Input
            type={inputType}
            value={this.props[property]}
            onChange={this.props.onChange.bind(null, property)}
            className="form-control"
          />
        </Col>
        {
          required ?
            <IconSVG
              name="icon-asterisk"
              className="text-danger"
            />
          : null
        }
      </FormGroup>
    );
  }

  renderTMSProperties() {
    let namespaces = NamespaceStore.getState().namespaces.map(namespace => namespace.name);

    return (
      <div>
        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate(`${PREFIX}.labels.namespace`)}</Label>
          </Col>
          <Col xs="8">
            <SelectWithOptions
              options={namespaces}
              value={this.props.namespace}
              onChange={this.props.onChange.bind(null, 'namespace')}
            />
          </Col>
        </FormGroup>
        {this.renderFormGroupProperty('topic', true)}
      </div>
    );
  }

  renderSQSProperties() {
    return (
      <div>
        {this.renderFormGroupProperty('region', true)}
        {this.renderFormGroupProperty('accessKey', true, 'password')}
        {this.renderFormGroupProperty('accessId', true, 'password')}
        {this.renderFormGroupProperty('queueName', true)}
        {this.renderFormGroupProperty('endpoint')}
      </div>
    );
  }

  renderWebsocketProperties() {
    return (
      <div>
        {this.renderFormGroupProperty('connection', true)}
        {this.renderFormGroupProperty('sslKeystoreFilePath')}
        {this.renderFormGroupProperty('sslKeystorePassword', false, 'password')}
        {this.renderFormGroupProperty('sslKeystoreKeyPassword', false, 'password')}
        {this.renderFormGroupProperty('sslKeystoreType')}
        {this.renderFormGroupProperty('sslTruststoreFilePath')}
        {this.renderFormGroupProperty('sslTruststorePassword', false, 'password')}
        {this.renderFormGroupProperty('sslTruststoreType')}
      </div>
    );
  }

  renderMapRStreamProperties() {
    return (
      <div>
        {this.renderFormGroupProperty('mapRTopic', true)}
        {this.renderFormGroupProperty('keySerdes', true)}
        {this.renderFormGroupProperty('valueSerdes', true)}
      </div>
    );
  }

  renderTypeProperties() {
    if (this.props.type === 'tms') {
      return this.renderTMSProperties();
    } else if (this.props.type === 'sqs') {
      return this.renderSQSProperties();
    } else if (this.props.type === 'websocket') {
      return this.renderWebsocketProperties();
    } else {
      return this.renderMapRStreamProperties();
    }
  }

  render() {
    return (
      <div className="microservice-queue-container">
        <div className="col-xs-10 microservice-queue-input-container">
          <Form
            className="form-horizontal"
            onSubmit={(e) => {
              e.preventDefault();
              return false;
            }}
          >
            <FormGroup row>
              <Col xs="3">
                <Label className="control-label">{T.translate('commons.nameLabel')}</Label>
              </Col>
              <Col xs="8">
                <Input
                  type="text"
                  value={this.props.name}
                  autoFocus
                  onChange={this.props.onChange.bind(null, 'name')}
                  className="form-control"
                />
              </Col>
              <IconSVG
                name="icon-asterisk"
                className="text-danger"
              />
            </FormGroup>
            <FormGroup row>
              <Col xs="3">
                <Label className="control-label">{T.translate('commons.typeLabel')}</Label>
              </Col>
              <Col xs="8">
                <SelectWithOptions
                  options={defaultQueueTypes}
                  value={this.props.type}
                  onChange={this.props.onChange.bind(null, 'type')}
                />
              </Col>
              <IconSVG
                name="icon-asterisk"
                className="text-danger"
              />
            </FormGroup>
            {this.renderTypeProperties()}
          </Form>
        </div>
        <div className="col-xs-2 microservice-queue-buttons-container">
          <button
            className="btn add-row-btn btn-link"
            onClick={this.props.onAdd}
          >
            <IconSVG name="icon-plus" />
          </button>
          <button
            className="btn remove-row-btn btn-link"
            onClick={this.props.onRemove}
          >
            <IconSVG
              className="text-danger"
              name="icon-trash"
            />
          </button>
        </div>
      </div>
    );
  }
}
