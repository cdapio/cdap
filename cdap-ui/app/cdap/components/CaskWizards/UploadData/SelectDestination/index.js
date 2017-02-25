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
import { connect, Provider } from 'react-redux';
import UploadDataStore from 'services/WizardStores/UploadData/UploadDataStore';
import UploadDataActions from 'services/WizardStores/UploadData/UploadDataActions';
import SelectWithOptions from 'components/SelectWithOptions';
import {Input, Form, FormGroup, Col, Label} from 'reactstrap';
import {UncontrolledTooltip} from 'components/UncontrolledComponents';
require('./SelectDestination.scss');
import T from 'i18n-react';

const mapStateToDestinationTypeProps = (state) => {
  return {
    options: state.selectdestination.types,
    value: state.selectdestination.type
  };
};
const mapStateToDestinationNameProps = (state) => {
  return {
    value: state.selectdestination.name,
    placeholder: T.translate('features.Wizard.UploadData.Step2.dataentitynameplaceholder')
  };
};
const mapDispatchToDestinationTypeProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: UploadDataActions.setDestinationType,
        payload: {
          type: e.target.value
        }
      });
    }
  };
};
const mapDispatchToDestinationNameProps = (dispatch) => {
  return {
    onChange: (e) => {
      dispatch({
        type: UploadDataActions.setDestinationName,
        payload: {
          name: e.target.value
        }
      });
    }
  };
};

let DestinationInput = ({value, placeholder, onChange}) => (
  <div className="destination-input">
    <Input
      value={value}
      placeholder={placeholder}
      onChange={onChange}
    />
    <i
      id="upload-data-destination-tooltip"
      className="fa fa-exclamation-circle btn-link"
    />
    <UncontrolledTooltip
      target="upload-data-destination-tooltip"
    >
      {T.translate('features.Wizard.UploadData.Step2.tooltiptext')}
    </UncontrolledTooltip>
  </div>
);
DestinationInput.propTypes = {
  value: PropTypes.string,
  placeholder: PropTypes.string,
  onChange: PropTypes.func
};

let DestinationType = connect(
  mapStateToDestinationTypeProps,
  mapDispatchToDestinationTypeProps
)(SelectWithOptions);
let DestinationName = connect(
  mapStateToDestinationNameProps,
  mapDispatchToDestinationNameProps
)(DestinationInput);

export default function SelectDestination() {
  return (
    <Provider store={UploadDataStore}>
      <Form
        className="form-horizontal select-destination-step"
         onSubmit={(e) => {e.stopPropagation(); e.preventDefault();}}
      >
      <FormGroup row>
        <Col xs="3">
          <Label className="control-label">{T.translate('features.Wizard.UploadData.Step2.destinationtype')}</Label>
        </Col>
        <Col xs="7">
          <DestinationType />
        </Col>
      </FormGroup>
      <FormGroup row>
        <Col xs="3">
          <Label className="control-label">{T.translate('features.Wizard.UploadData.Step2.destinationname')}</Label>
        </Col>
        <Col xs="7">
          <DestinationName />
        </Col>
      </FormGroup>
    </Form>
    </Provider>
  );
}
