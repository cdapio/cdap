/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, { PropTypes } from 'react';
import { connect, Provider } from 'react-redux';
import MicroserviceUploadActions  from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';
import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import { Label, Form, FormGroup, Col, Input } from 'reactstrap';
import InputWithValidations from 'components/InputWithValidations';
import SelectWithOptions from 'components/SelectWithOptions';
import { preventPropagation } from 'services/helpers';
import T from 'i18n-react';

require('./GeneralInfoStep.scss');

const mapStateToInstanceNameProps = (state) => {
  return {
    value: state.general.instanceName,
    type: 'text',
    placeholder: T.translate('features.Wizard.MicroserviceUpload.Step1.instanceNamePlaceholder')
  };
};
const mapStateToMicroserviceDescritionProps = (state) => {
  return {
    value: state.general.description,
    type: 'textarea',
    rows: '3',
    placeholder: T.translate('features.Wizard.MicroserviceUpload.Step1.descriptionPlaceholder')
  };
};
const mapStateToMicroserviceVersionProps = (state) => {
  return {
    value: state.general.version,
    type: 'number',
    min: 1,
    placeholder: T.translate('features.Wizard.MicroserviceUpload.Step1.versionPlaceholder')
  };
};
const mapStateToMicroserviceOptionProps = (state) => {
  return {
    value: state.general.microserviceOption,
    options: state.general.defaultMicroserviceOptions
  };
};
const mapStateToNewMicroserviceNameProps = (state) => {
  return {
    value: state.general.newMicroserviceName,
    showNewMicroserviceTextbox: state.general.showNewMicroserviceTextbox
  };
};
const mapStateToGeneralStepSummaryProps = (state) => {
  return {
    microserviceOption: state.general.microserviceOption,
    newMicroserviceName: state.general.newMicroserviceName,
    showNewMicroserviceTextbox: state.general.showNewMicroserviceTextbox,
    instanceName: state.general.instanceName,
    version: state.general.version
  };
};
const mapStateToGeneralStepReadOnlyProps = (state) => {
  return {
    disabled: state.general.__readOnly
  };
};

const mapDispatchToInstanceNameProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: MicroserviceUploadActions.setInstanceName,
        payload: {instanceName: e.target.value}
      });
    }
  };
};
const mapDispatchToMicroserviceDescriptionProps = (dispatch) => {
  return {
    onChange: (e) => (dispatch({
      type: MicroserviceUploadActions.setDescription,
      payload: {description: e.target.value}
    }))
  };
};
const mapDispatchToToMicroserviceVersion = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: MicroserviceUploadActions.setVersion,
        payload: {version: e.target.value}
      });
    }
  };
};
const mapDispatchToMicroserviceOptionProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: MicroserviceUploadActions.setMicroserviceOption,
        payload: {microserviceOption: e.target.value}
      });
    }
  };
};
const mapDispatchToNewMicroserviceNameProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: MicroserviceUploadActions.setNewMicroserviceName,
        payload: {newMicroserviceName: e.target.value}
      });
    }
  };
};

let NewMicroserviceTextbox = ({showNewMicroserviceTextbox, value, onChange}) => {
  if (!showNewMicroserviceTextbox) { return null; }

  return (
    <InputWithValidations
      value = {value}
      onChange = {onChange}
      className = "new-microservice-textbox"
      type = 'text'
      placeholder = {T.translate('features.Wizard.MicroserviceUpload.Step1.newMicroservicePlaceholder')}
    />
  );
};

NewMicroserviceTextbox.propTypes = {
  value: PropTypes.string,
  onChange: PropTypes.function,
  showNewMicroserviceTextbox: PropTypes.bool
};

let Summary = ({microserviceOption, newMicroserviceName, showNewMicroserviceTextbox, instanceName, version}) => {
  let microserviceName = microserviceOption;
  if (showNewMicroserviceTextbox) {
    microserviceName = newMicroserviceName;
  }
  return (
    <span>
      {T.translate('features.Wizard.MicroserviceUpload.Step1.summary', { microserviceName, instanceName, version })}
    </span>
  );
};

Summary.propTypes = {
  microserviceOption: PropTypes.string,
  newMicroserviceName: PropTypes.string,
  showNewMicroserviceTextbox: PropTypes.string,
  instanceName: PropTypes.string,
  version: PropTypes.number
};

let Fieldset = ({disabled, children}) => {
  return (
    <fieldset disabled={disabled}>
      {children}
    </fieldset>
  );
};

Fieldset.propTypes = {
  disabled: PropTypes.bool,
  children: PropTypes.node
};

// FIXME: Should pass validationError to the InputWithValidations, or just switch
// to using Input if we don't need validations

const InputMicroserviceInstanceName = connect(
  mapStateToInstanceNameProps,
  mapDispatchToInstanceNameProps
)(InputWithValidations);
const InputMicroserviceDescription = connect(
  mapStateToMicroserviceDescritionProps,
  mapDispatchToMicroserviceDescriptionProps
)(Input);
const InputMicroserviceVersion = connect(
  mapStateToMicroserviceVersionProps,
  mapDispatchToToMicroserviceVersion
)(InputWithValidations);
const SelectMicroserviceOption = connect(
  mapStateToMicroserviceOptionProps,
  mapDispatchToMicroserviceOptionProps
)(SelectWithOptions);
NewMicroserviceTextbox = connect(
  mapStateToNewMicroserviceNameProps,
  mapDispatchToNewMicroserviceNameProps
)(NewMicroserviceTextbox);
Summary = connect(
  mapStateToGeneralStepSummaryProps,
  null
)(Summary);
const CustomFieldset = connect(
  mapStateToGeneralStepReadOnlyProps,
  null
)(Fieldset);

export default function GeneralInfoStep() {
  return (
    <Provider store={MicroserviceUploadStore}>
      <Form
        className="form-horizontal general-info-step"
        onSubmit={(e) => {
          preventPropagation(e);
          return false;
        }}
      >
        <CustomFieldset>
          <FormGroup row>
            <Col xs="3">
              <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step1.instanceNameLabel')}</Label>
            </Col>
            <Col xs="7">
              <InputMicroserviceInstanceName />
            </Col>
            <i className="fa fa-asterisk text-danger float-xs-left" />
          </FormGroup>
          <FormGroup row>
            <Col xs="3">
              <Label className="control-label">{T.translate('commons.descriptionLabel')}</Label>
            </Col>
            <Col xs="7">
              <InputMicroserviceDescription />
            </Col>
          </FormGroup>
          <FormGroup row>
            <Col sm="3">
              <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step1.versionLabel')}</Label>
            </Col>
            <Col sm="7">
              <InputMicroserviceVersion />
            </Col>
            <i className="fa fa-asterisk text-danger float-xs-left" />
          </FormGroup>
          <FormGroup row>
            <Col xs="3">
              <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step1.microserviceOptionLabel')}</Label>
            </Col>
            <Col xs="7">
              <SelectMicroserviceOption />
              <NewMicroserviceTextbox />
            </Col>
            <i className="fa fa-asterisk text-danger float-xs-left" />
          </FormGroup>
          <div className="step-summary">
            <Label className="summary-label">{T.translate('features.Wizard.MicroserviceUpload.summaryLabel')}</Label>
            <Summary />
          </div>
        </CustomFieldset>
      </Form>
    </Provider>
  );
}
