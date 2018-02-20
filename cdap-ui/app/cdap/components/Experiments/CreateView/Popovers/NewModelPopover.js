/*
 * Copyright © 2018 Cask Data, Inc.
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
  onModelNameChange,
  onModelDescriptionChange,
  createModel
} from 'components/Experiments/store/CreateExperimentActionCreator';
import {POPOVER_TYPES} from 'components/Experiments/store/createExperimentStore';

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

const CreateModelBtn = ({state, createModel}) => {
  const isAddModelBtnEnabled = () => state.name.length && state.description.length;
  return (
    <button
      className="btn btn-primary"
      onClick={createModel}
      disabled={!isAddModelBtnEnabled()}
    >
      Create Model
    </button>
  );
};
CreateModelBtn.propTypes = {
  state: PropTypes.object,
  createModel: PropTypes.func
};

const ExperimentMetadata = ({experimentOutcome, experimentDescription}) => {
  return (
    <div className="experiment-metadata-popover">
      <Col xs="12">
        <Row>
          <Col xs="6">Outcome:</Col>
          <Col xs="6">{experimentOutcome}</Col>
        </Row>
        <Row>
          <Col xs="6">Description:</Col>
          <Col xs="6">{experimentDescription}</Col>
        </Row>
      </Col>
    </div>
  );
};
ExperimentMetadata.propTypes = {
  experimentOutcome: PropTypes.string,
  experimentDescription: PropTypes.string
};

const NewModelPopoverWrapper = ({popover, experimentName}) => {
  if (popover !== POPOVER_TYPES.MODEL) {
    return null;
  }
  return (
    <div className="new-model-popover">
      <FormGroup row>
        <Col xs="12">
          <Label className="control-label"> Create model under the experiment </Label>
        </Col>
        <Col xs="12">
          <Input disabled value={experimentName} />
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
  popover: PropTypes.string,
  experimentName: PropTypes.string
};

const mapStateToModelNameProps = (state) => ({modelName: state.model_create.name});
const mapDispatchToModelNameProps = () => ({ onModelNameChange });
const mapStateToModelDescriptionProps = (state) => ({ modelDescription: state.model_create.description });
const mapDispatchToModelDescriptionProps = () => ({ onModelDescriptionChange });
const mapStateToCreateModelBtnProps = (state) => ({ state: state.model_create});
const mapDispatchToCreateModelBtnProps = () => ({ createModel });
const mapStateToExperimentMetadataProps = (state) => ({
  experimentOutcome: state.experiments_create.outcome,
  experimentDescription: state.experiments_create.description
});
const mapNMPWStateToProps = (state) => ({
  popover: state.experiments_create.popover,
  experimentName: state.experiments_create.name
});

const ConnectedModelName = connect(mapStateToModelNameProps, mapDispatchToModelNameProps)(ModelName);
const ConnectedModelDescription = connect(mapStateToModelDescriptionProps, mapDispatchToModelDescriptionProps)(ModelDescription);
const ConnectedCreateModelBtn = connect(mapStateToCreateModelBtnProps, mapDispatchToCreateModelBtnProps)(CreateModelBtn);
const ConnectedExperimentMetadata = connect(mapStateToExperimentMetadataProps)(ExperimentMetadata);
const ConnectedNewModelPopoverWrapper = connect(mapNMPWStateToProps)(NewModelPopoverWrapper);

export default ConnectedNewModelPopoverWrapper;
