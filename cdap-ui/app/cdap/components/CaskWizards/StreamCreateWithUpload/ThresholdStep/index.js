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
import React, {PropTypes} from 'react';
import {connect, Provider} from 'react-redux';
import {Input, FormGroup, Form} from 'reactstrap';
import T from 'i18n-react';

require('./ThresholdStep.less');
import CreateStreamWithUploadActions  from 'services/WizardStores/CreateStreamWithUpload/CreateStreamWithUploadActions';
import CreateStreamWithUploadStore from 'services/WizardStores/CreateStreamWithUpload/CreateStreamWithUploadStore';
const mapStateToStreamThresholdProps = (state) => {
  return {
    value: parseInt(state.threshold.value, 10),
    type: 'text',
    defaultValue: parseInt(state.threshold.value, 10),
    placeholder: 'Threshold'
  };
};
const mapDispatchToStreamThresholdProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: CreateStreamWithUploadActions.setThreshold,
        payload: {threshold: e.target.value}
      });
    }
  };
};
let ThresholdTextBox = ({value, onChange}) => {
  return (
    <FormGroup className="threshold-step">
      <Input
        value={value}
        type="number"
        onChange={onChange}
      />
    <h3>{T.translate('features.Wizard.StreamCreate.Step3.mblabel')}</h3>
    </FormGroup>
  );
};
ThresholdTextBox.propTypes = {
  value: PropTypes.number,
  onChange: PropTypes.func
};
ThresholdTextBox = connect(
  mapStateToStreamThresholdProps,
  mapDispatchToStreamThresholdProps
)(ThresholdTextBox);
export default function ThresholdStep() {
  return (
    <Provider store={CreateStreamWithUploadStore}>
      <Form className="form-horizontal">
        <ThresholdTextBox />
        <p>
          {T.translate('features.Wizard.StreamCreate.Step3.thresholdlabel')}
        </p>
      </Form>
    </Provider>
  );
}
