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

import React from 'react';
import { connect, Provider } from 'react-redux';
import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import MicroserviceUploadActions from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';
import { Col, Label, FormGroup, Form, Input } from 'reactstrap';
import InputWithValidations from 'components/InputWithValidations';
import T from 'i18n-react';

const mapStateToMicroserviceInstancesProps = (state) => {
  return {
    value: state.configure.instances,
    type: 'number',
    min: '1',
    placeholder: T.translate('features.Wizard.MicroserviceUpload.Step4.instancesPlaceholder')
  };
};
const mapStateToMicroserviceVCoresProps = (state) => {
  return {
    value: state.configure.vcores,
    type: 'number',
    min: '1',
    placeholder: T.translate('features.Wizard.MicroserviceUpload.Step4.vcoresPlaceholder')
  };
};
const mapStateToMicroserviceMemoryProps = (state) => {
  return {
    value: state.configure.memory,
    type: 'number',
    min: '1',
    placeholder: T.translate('features.Wizard.MicroserviceUpload.Step4.memoryPlaceholder')
  };
};
const mapStateToMicroserviceThresholdProps = (state) => {
  return {
    value: state.configure.ethreshold,
    type: 'number',
    min: '1',
    placeholder: T.translate('features.Wizard.MicroserviceUpload.Step4.thresholdPlaceholder')
  };
};

const mapDispatchToMicroserviceInstancesProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: MicroserviceUploadActions.setInstances,
        payload: {instances: e.target.value}
      });
    }
  };
};
const mapDispatchToMicroserviceVCoresProps = (dispatch) => {
  return {
    onChange: (e) => (dispatch({
      type: MicroserviceUploadActions.setVCores,
      payload: {vcores: e.target.value}
    }))
  };
};
const mapDispatchToMicroserviceMemoryProps = (dispatch) => {
  return {
    onChange: (e) => (dispatch({
      type: MicroserviceUploadActions.setMemory,
      payload: {memory: e.target.value}
    }))
  };
};
const mapDispatchToMicroserviceThresholdProps = (dispatch) => {
  return {
    onChange: (e) => (dispatch({
      type: MicroserviceUploadActions.setThreshold,
      payload: {ethreshold: e.target.value}
    }))
  };
};

const InputMicroserviceInstances = connect(
  mapStateToMicroserviceInstancesProps,
  mapDispatchToMicroserviceInstancesProps
)(InputWithValidations);
const InputMicroserviceVCores = connect(
  mapStateToMicroserviceVCoresProps,
  mapDispatchToMicroserviceVCoresProps
)(InputWithValidations);
const InputMicroserviceMemory = connect(
  mapStateToMicroserviceMemoryProps,
  mapDispatchToMicroserviceMemoryProps
)(InputWithValidations);
const InputMicroserviceThreshold = connect(
  mapStateToMicroserviceThresholdProps,
  mapDispatchToMicroserviceThresholdProps
)(Input);


export default function ConfigureStep() {
  return (
    <Provider store={MicroserviceUploadStore}>
      <Form
        className="form-horizontal general-info-step"
        onSubmit={(e) => {
          e.preventDefault();
          return false;
        }}
      >
        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step4.instancesLabel')}</Label>
          </Col>
          <Col xs="7">
            <InputMicroserviceInstances />
          </Col>
          <i className="fa fa-asterisk text-danger float-xs-left"/>
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step4.vcoresLabel')}</Label>
          </Col>
          <Col xs="7">
            <InputMicroserviceVCores />
          </Col>
          <i className="fa fa-asterisk text-danger float-xs-left"/>
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step4.memoryLabel')}</Label>
          </Col>
          <Col xs="7">
            <InputMicroserviceMemory />
          </Col>
          <i className="fa fa-asterisk text-danger float-xs-left"/>
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step4.thresholdLabel')}</Label>
          </Col>
          <Col xs="7">
            <InputMicroserviceThreshold />
          </Col>
          <i className="fa fa-asterisk text-danger float-xs-left"/>
        </FormGroup>
      </Form>
    </Provider>
  );
}
