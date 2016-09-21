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

require('./ThresholdStep.less');
import CreateStreamActions  from 'services/WizardStores/CreateStream/CreateStreamActions';
import CreateStreamStore from 'services/WizardStores/CreateStream/CreateStreamStore';
const mapStateToStreamThresholdProps = (state) => {
  return {
    value: state.threshold.value,
    type: 'text',
    defaultValue: state.threshold.value,
    placeholder: 'Threshold'
  };
};
const mapDispatchToStreamThresholdProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: CreateStreamActions.setThreshold,
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
        onChange={onChange}
      />
    <h3>Megabytes (MB)</h3>
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
    <Provider store={CreateStreamStore}>
      <Form className="form-horizontal">
        <ThresholdTextBox />
        <p>
          The stream will notify any observers upon reaching this threshold to start processing of the data in this Stream
        </p>
      </Form>
    </Provider>
  );
}
