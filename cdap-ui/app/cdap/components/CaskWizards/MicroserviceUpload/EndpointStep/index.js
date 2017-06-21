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
import {connect, Provider} from 'react-redux';
import {Col, FormGroup, Label, Form} from 'reactstrap';
import InputWithValidations from 'components/InputWithValidations';
import T from 'i18n-react';

import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import MicroserviceUploadActions from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';
import DSVInboundQueues from './DSVInboundQueues.js';
import DSVOutboundQueues from './DSVOutboundQueues.js';

const mapStateToFetchSizeProps = (state) => {
  return {
    value: state.endpoints.fetch,
    type: 'number',
    min: '1',
    placeholder: T.translate('features.Wizard.MicroserviceUpload.Step5.fetchPlaceholder')
  };
};

const mapDispatchToFetchSizeProps = (dispatch) => {
  return {
    onChange: (e) => (dispatch({
      type: MicroserviceUploadActions.setFetchSize,
      payload: { fetchSize: e.target.value}
    }))
  };
};

const InputFetchSize = connect(
  mapStateToFetchSizeProps,
  mapDispatchToFetchSizeProps
)(InputWithValidations);

export default function EndpointStep() {
  return (
    <Provider store={MicroserviceUploadStore}>
      <Form
        className="form-horizontal"
        onSubmit={(e) => {
          e.preventDefault();
          return false;
        }}
      >
        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step5.fetchLabel')}</Label>
          </Col>
          <Col xs="7">
            <InputFetchSize />
          </Col>
        </FormGroup>

        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step5.inboundLabel')}</Label>
          </Col>
          <Col xs="7">
            <DSVInboundQueues />
          </Col>
        </FormGroup>
        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">{T.translate('features.Wizard.MicroserviceUpload.Step5.outboundLabel')}</Label>
          </Col>
          <Col xs="7">
            <DSVOutboundQueues />
          </Col>
        </FormGroup>
      </Form>
    </Provider>
  );
}
