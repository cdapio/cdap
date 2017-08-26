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
import { Label, Form, FormGroup, Col, Input } from 'reactstrap';
import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import MicroserviceUploadActions from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';
import MicroserviceQueueEditor from 'components/CaskWizards/MicroserviceUpload/MicroserviceQueueEditor';
import { preventPropagation } from 'services/helpers';
import T from 'i18n-react';

const mapStateToFetchSizeProps = (state) => {
  return {
    value: state.inboundQueues.fetch,
    type: 'number',
    min: 1
  };
 };

 const mapDispatchToFetchSizeProps = (dispatch) => {
  return {
    onChange: (e) => (dispatch({
      type: MicroserviceUploadActions.setFetchSize,
      payload: { fetchSize: e.target.value }
    }))
  };
 };

 const InputFetchSize = connect(
   mapStateToFetchSizeProps,
   mapDispatchToFetchSizeProps
 )(Input);

const mapStateToInboundQueuesProps = (state) => {
  return {
    values: state.inboundQueues.queues
  };
};

const mapDispatchToInboundQueuesProps = (dispatch) => {
  return {
    onChange: (values) => (dispatch({
      type: MicroserviceUploadActions.setInboundQueues,
      payload: { inboundQueues: values }
    }))
  };
};

const MicroserviceQueueWrapper = connect(
  mapStateToInboundQueuesProps,
  mapDispatchToInboundQueuesProps
)(MicroserviceQueueEditor);

export default function InboundQueueStep() {
  return (
    <Provider store={MicroserviceUploadStore}>
      <Form
        className="form-horizontal"
        onSubmit={(e) => {
          preventPropagation(e);
          return false;
        }}
      >
        <FormGroup row className="inbound-queue-fetch-size">
          <Col xs="3">
            <Label className="control-label">
              {T.translate('features.Wizard.MicroserviceUpload.Step5.fetchLabel')}
            </Label>
          </Col>
          <Col xs="7">
            <InputFetchSize />
          </Col>
        </FormGroup>
        <MicroserviceQueueWrapper className="inbound-queue-wrapper" />
      </Form>
    </Provider>
  );
}
