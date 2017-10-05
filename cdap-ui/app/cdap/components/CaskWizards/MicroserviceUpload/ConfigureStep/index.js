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

import PropTypes from 'prop-types';

import React from 'react';
import { connect, Provider } from 'react-redux';
import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import MicroserviceUploadActions from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';
import { Col, Label, FormGroup, Form } from 'reactstrap';
import InputWithValidations from 'components/InputWithValidations';
import { preventPropagation } from 'services/helpers';
import T from 'i18n-react';

const mapStateToMicroserviceInstancesProps = (state) => {
  return {
    value: state.configure.instances,
    type: 'number',
    min: 1,
    placeholder: T.translate('features.Wizard.MicroserviceUpload.Step4.instancesPlaceholder')
  };
};
const mapStateToMicroserviceVCoresProps = (state) => {
  return {
    value: state.configure.vcores,
    type: 'number',
    min: 1,
    placeholder: T.translate('features.Wizard.MicroserviceUpload.Step4.vcoresPlaceholder')
  };
};
const mapStateToMicroserviceMemoryProps = (state) => {
  return {
    value: state.configure.memory,
    type: 'number',
    min: 1,
    placeholder: T.translate('features.Wizard.MicroserviceUpload.Step4.memoryPlaceholder')
  };
};
const mapStateToMicroserviceThresholdProps = (state) => {
  return {
    value: state.configure.ethreshold,
    type: 'number',
    min: 1,
    placeholder: T.translate('features.Wizard.MicroserviceUpload.Step4.thresholdPlaceholder')
  };
};
const mapStateToConfigureStepSummaryProps = (state) => {
  return {
    instanceName: state.general.instanceName,
    instances: state.configure.instances,
    vcores: state.configure.vcores,
    memory: state.configure.memory,
    ethreshold: state.configure.ethreshold,
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

let Summary = ({instanceName, instances, vcores, memory, ethreshold}) => {
  let instancesWithCount = T.translate('features.Wizard.MicroserviceUpload.Step4.summary.count.instances', { context: instances });
  let vcoresWithCount = T.translate('features.Wizard.MicroserviceUpload.Step4.summary.count.vcores', { context: vcores });
  let memoryWithCount = T.translate('features.Wizard.MicroserviceUpload.Step4.summary.count.memory', { context: memory });
  let ethresholdWithCount = T.translate('features.Wizard.MicroserviceUpload.Step4.summary.count.ethreshold', { context: ethreshold });
  return (
    <span>
      {T.translate('features.Wizard.MicroserviceUpload.Step4.summary.text', { instanceName, instancesWithCount, vcoresWithCount, memoryWithCount, ethresholdWithCount })}
    </span>
  );
};

Summary.propTypes = {
  instanceName: PropTypes.string,
  instances: PropTypes.number,
  vcores: PropTypes.number,
  memory: PropTypes.number,
  ethreshold: PropTypes.number
};

// FIXME: Should pass validationError to the InputWithValidations, or just switch
// to using Input if we don't need validations

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
)(InputWithValidations);
Summary = connect(
  mapStateToConfigureStepSummaryProps,
  null
)(Summary);


export default function ConfigureStep() {
  return (
    <Provider store={MicroserviceUploadStore}>
      <Form
        className="form-horizontal configure-step"
        onSubmit={(e) => {
          preventPropagation(e);
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
          <i className="fa fa-asterisk text-danger float-xs-left" />
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step4.vcoresLabel')}</Label>
          </Col>
          <Col xs="7">
            <InputMicroserviceVCores />
          </Col>
          <i className="fa fa-asterisk text-danger float-xs-left" />
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step4.memoryLabel')}</Label>
          </Col>
          <Col xs="7">
            <InputMicroserviceMemory />
          </Col>
          <i className="fa fa-asterisk text-danger float-xs-left" />
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step4.thresholdLabel')}</Label>
          </Col>
          <Col xs="7">
            <InputMicroserviceThreshold />
          </Col>
          <i className="fa fa-asterisk text-danger float-xs-left" />
        </FormGroup>
        <div className="step-summary">
          <Label className="summary-label">{T.translate('features.Wizard.MicroserviceUpload.summaryLabel')}</Label>
          <Summary />
        </div>
      </Form>
    </Provider>
  );
}
