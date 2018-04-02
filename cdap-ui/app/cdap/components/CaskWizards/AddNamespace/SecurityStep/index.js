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
import AddNamespaceStore from 'services/WizardStores/AddNamespace/AddNamespaceStore';
import AddNamespaceActions  from 'services/WizardStores/AddNamespace/AddNamespaceActions';
import InputWithValidations from 'components/InputWithValidations';
import {Provider, connect} from 'react-redux';
import T from 'i18n-react';

// Principal
const mapStateToPrincipalProps = (state) => {
  return {
    value: state.security.principal,
    type: 'text',
    placeholder: T.translate('features.Wizard.Add-Namespace.Step3.principal-placeholder'),
    disabled: state.editableFields.fields.indexOf('principal') === -1
  };
};

const mapDispatchToPrincipalProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: AddNamespaceActions.setPrincipal,
        payload: { principal : e.target.value }
      });
    }
  };
};

// KeytabURI
const mapStateTokeytabURIProps = (state) => {
  return {
    value: state.security.keyTab,
    type: 'text',
    placeholder: T.translate('features.Wizard.Add-Namespace.Step3.keytab-uri-placeholder'),
    disabled: state.editableFields.fields.indexOf('keyTab') === -1
  };
};

const mapDispatchTokeytabURIProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: AddNamespaceActions.setKeytab,
        payload: { keyTab : e.target.value }
      });
    }
  };
};

const InputPrincipal = connect(
  mapStateToPrincipalProps,
  mapDispatchToPrincipalProps
)(InputWithValidations);

const InputKeytabURI = connect(
  mapStateTokeytabURIProps,
  mapDispatchTokeytabURIProps
)(InputWithValidations);

export default function PreferencesStep() {
  return (
    <Provider store={AddNamespaceStore}>
      <Form
        className="form-horizontal"
        onSubmit={(e) => {
          e.preventDefault();
          return false;
        }}
      >
        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">
              {T.translate('features.Wizard.Add-Namespace.Step3.principal-label')}
            </Label>
          </Col>
          <Col xs="7">
            <InputPrincipal />
          </Col>
        </FormGroup>
        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">
              {T.translate('features.Wizard.Add-Namespace.Step3.keytab-uri-label')}
            </Label>
          </Col>
          <Col xs="7">
            <InputKeytabURI />
          </Col>
        </FormGroup>
      </Form>
    </Provider>
  );
}
