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

import PropTypes from 'prop-types';
import React from 'react';
import {connect} from 'react-redux';
import {FormGroup, Label, Col, Input, Row} from 'reactstrap';
import {
  setExperimentCreated,
  onModelNameChange,
  onModelDescriptionChange,
  setModelCreated
} from 'components/Experiments/store/ActionCreator';

const ModelName = ({modelName, onModelNameChange}) => {
  return (
    <FormGroup row>
      <Col xs="12">
        <Label className="control-label">Model Name </Label>
      </Col>
      <Col xs="12">
        <Input value={modelName} onChange={onModelNameChange} />
      </Col>
    </FormGroup>
  );
};
ModelName.propTypes = {
  modelName: PropTypes.string,
  onModelNameChange: PropTypes.func
};

const ModelDescription = ({modelDescription, onModelDescriptionChange}) => {
  return (
    <FormGroup row>
      <Col xs="12">
        <Label className="control-label">Model Description </Label>
      </Col>
      <Col xs="12">
        <Input
          type="textarea"
          value={modelDescription}
          onChange={onModelDescriptionChange}
        />
      </Col>
    </FormGroup>
  );
};
ModelDescription.propTypes = {
  modelDescription: PropTypes.string,
  onModelDescriptionChange: PropTypes.func
};

const CreateModelBtn = ({state, setModelCreated}) => {
  const isAddModelBtnEnabled = () => state.name.length && state.description.length;
  return (
    <button
      className="btn btn-primary"
      onClick={setModelCreated}
      disabled={!isAddModelBtnEnabled()}
    >
      Create Model
    </button>
  );
};
CreateModelBtn.propTypes = {
  state: PropTypes.object,
  setModelCreated: PropTypes.func
};

const ExperimentMetadata = ({experimentOutcome, experimentDescription, setExperimentCreated}) => {
  return (
    <div className="experiment-metadata">
      <Col xs="12">
        <Row>
          <Col xs="6">Outcome:</Col>
          <Col xs="6">{experimentOutcome}</Col>
        </Row>
        <Row>
          <Col xs="6">Description:</Col>
          <Col xs="6">{experimentDescription}</Col>
        </Row>
        <div
          className="btn-link"
          onClick={setExperimentCreated.bind(null, false)}
        >
          Edit
        </div>
      </Col>
    </div>
  );
};
ExperimentMetadata.propTypes = {
  experimentOutcome: PropTypes.string,
  experimentDescription: PropTypes.string,
  setExperimentCreated: PropTypes.func
};

const NewModelPopoverWrapper = ({isExperimentCreated, experimentName}) => {
  if (!isExperimentCreated) {
    return null;
  }
  return (
    <div className="new-model-popover">
      <FormGroup row>
        <Col xs="12">
          <Label className="control-label"> Select where you want to add this model </Label>
        </Col>
        <Col xs="12">
          <Input value={experimentName} />
        </Col>
        <ConnectedExperimentMetadata />
      </FormGroup>
      <hr />
      <ConnectedModelName />
      <ConnectedModelDescription />
      <ConnectedCreateModelBtn />
    </div>
  );
};
NewModelPopoverWrapper.propTypes = {
  isExperimentCreated: PropTypes.bool,
  experimentName: PropTypes.string
};

const mapStateToModelNameProps = (state) => ({modelName: state.model_create.name});
const mapDispatchToModelNameProps = () => ({ onModelNameChange });
const mapStateToModelDescriptionProps = (state) => ({ modelDescription: state.model_create.description });
const mapDispatchToModelDescriptionProps = () => ({ onModelDescriptionChange });
const mapStateToCreateModelBtnProps = (state) => ({ state: state.model_create});
const mapDispatchToCreateModelBtnProps = () => ({ setModelCreated });
const mapStateToExperimentMetadataProps = (state) => ({
  experimentOutcome: state.experiments_create.outcome,
  experimentDescription: state.experiments_create.description
});
const mapDispatchToExperimentMetadataProps = () => ({ setExperimentCreated });
const mapNMPWStateToProps = (state) => ({
  isExperimentCreated: state.experiments_create.isExperimentCreated,
  experimentName: state.experiments_create.name
});

const ConnectedModelName = connect(mapStateToModelNameProps, mapDispatchToModelNameProps)(ModelName);
const ConnectedModelDescription = connect(mapStateToModelDescriptionProps, mapDispatchToModelDescriptionProps)(ModelDescription);
const ConnectedCreateModelBtn = connect(mapStateToCreateModelBtnProps, mapDispatchToCreateModelBtnProps)(CreateModelBtn);
const ConnectedExperimentMetadata = connect(mapStateToExperimentMetadataProps, mapDispatchToExperimentMetadataProps)(ExperimentMetadata);
const ConnectedNewModelPopoverWrapper = connect(mapNMPWStateToProps)(NewModelPopoverWrapper);

export default ConnectedNewModelPopoverWrapper;
