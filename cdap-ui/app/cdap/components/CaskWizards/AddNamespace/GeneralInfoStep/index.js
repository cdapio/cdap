/*
 * Copyright © 2016-2018 Cask Data, Inc.
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
import {Col, FormGroup, Label, Form} from 'reactstrap';
import AddNamespaceActions  from 'services/WizardStores/AddNamespace/AddNamespaceActions';
import AddNamespaceStore from 'services/WizardStores/AddNamespace/AddNamespaceStore';
import {Provider, connect} from 'react-redux';
import T from 'i18n-react';
import ValidatedInput from 'components/ValidatedInput';
import types from 'services/inputValidationTemplates';


var inputs = {
  name: {
    error: '',
    required: true,
    template: 'NAME',
    label: 'GeneralInfo Name',
  },
  description: {
    error: '',
    required: false,
    template: 'NAME',
    label: 'GeneralInfo Description',
  },
}

const getErrorMessage = (value, field) => {
  const isValid = types[inputs[field].template].validate(value);
  if (value && !isValid) {
    return types[inputs[field].template].getErrorMsg();
  } else {
    return '';
  }
}

// Namespace Name
const mapStateToNamespaceNameProps = (state) => {
  return {
    value: state.general.name,
    type: 'text',
    placeholder: T.translate('features.Wizard.Add-Namespace.Step1.name-placeholder'),
    disabled: state.editableFields.fields.indexOf('name') === -1,
    label:  inputs.name.label,
    inputInfo: types[inputs.name.template].getInfo(),
    validationError: inputs.name.error
  };
};


const mapDispatchToNamespaceNameProps = (dispatch) => {
  return {
    onChange: (e) => {
      inputs.name.error = getErrorMessage(e.target.value, 'name');
      dispatch({
        type: AddNamespaceActions.setName,
        payload: { name : e.target.value, name_valid: inputs.name.error !== '' ? false : true }
      });
    }

  };
};

// Namespace description
const mapStateToNamespaceDescriptionProps = (state) => {
  return {
    value: state.general.description,
    type: 'text',
    placeholder: T.translate('features.Wizard.Add-Namespace.Step1.description-placeholder'),
    disabled: state.editableFields.fields.indexOf('description') === -1,
    label:  inputs.description.label,
    inputInfo: types[inputs.description.template].getInfo(),
    validationError: inputs.description.error
  };
};

const mapDispatchToNamespaceDescriptionProps = (dispatch) => {
  return {
    onChange: (e) => {
      inputs.description.error = getErrorMessage(e.target.value, 'description');
      dispatch({
        type: AddNamespaceActions.setDescription,
        payload: { description: e.target.value, description_valid: inputs.description.error !== '' ? false : true  }
      });
    }
  };
};

const InputNamespaceName = connect(
  mapStateToNamespaceNameProps,
  mapDispatchToNamespaceNameProps
)(ValidatedInput);

const InputNamespaceDescription = connect(
  mapStateToNamespaceDescriptionProps,
  mapDispatchToNamespaceDescriptionProps
)(ValidatedInput);

export default function GeneralInfoStep() {
  return (
    <Provider store={AddNamespaceStore}>
      <Form
        className="form-horizontal general-info-step"
        onSubmit={(e) => {
          e.preventDefault();
          return false;
        }}
      >
          <FormGroup row>
            <Col xs="3">
              <Label className="control-label">{T.translate('features.Wizard.Add-Namespace.Step1.name-label')}</Label>
            </Col>
            <Col xs="7">
              <InputNamespaceName/>
            </Col>
            <span className="fa fa-asterisk text-danger float-xs-left" />
          </FormGroup>
          <FormGroup row>
            <Col xs="3">
              <Label className="control-label">{T.translate('features.Wizard.Add-Namespace.Step1.description-label')}</Label>
            </Col>
            <Col xs="7">
              <InputNamespaceDescription />
            </Col>
          </FormGroup>
      </Form>
    </Provider>
  );
}
