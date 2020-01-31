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
import React from 'react';
import PublishPipelineStore from 'services/WizardStores/PublishPipeline/PublishPipelineStore';
import PublishPipelineAction from 'services/WizardStores/PublishPipeline/PublishPipelineActions';
import { connect, Provider } from 'react-redux';
import { Input, Form, FormGroup, Col, Label } from 'reactstrap';
import T from 'i18n-react';
require('./MetadataStep.scss');

const mapStateToPipelineNameProps = (state) => {
  return {
    value: state.pipelinemetadata.name,
    placeholder: T.translate('features.Wizard.PublishPipeline.pipelinenameplaceholder'),
  };
};
const mapDispatchToPipelineProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: PublishPipelineAction.setPipelineName,
        payload: {
          name: e.target.value,
        },
      });
    },
  };
};

const PipelineName = connect(mapStateToPipelineNameProps, mapDispatchToPipelineProps)(Input);

export default function MetadataStep() {
  return (
    <Provider store={PublishPipelineStore}>
      <Form
        className="form-horizontal pipeline-publish-metadata-step"
        onSubmit={(e) => {
          e.stopPropagation();
          e.preventDefault();
        }}
      >
        <FormGroup row>
          <Col xs="3">
            <Label className="control-label">
              {T.translate('features.Wizard.PublishPipeline.pipelinenameplaceholder')}
            </Label>
          </Col>
          <Col xs="7">
            <PipelineName />
          </Col>
        </FormGroup>
      </Form>
    </Provider>
  );
}
