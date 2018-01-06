/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import {FormGroup, Label, Col, Input} from 'reactstrap';
import {
  onExperimentNameChange,
  onExperimentDescriptionChange,
  onExperimentOutcomeChange,
  createExperiment
} from 'components/Experiments/store/ActionCreator';
import IconSVG from 'components/IconSVG';

const ExperimentName = ({name, onNameChange, isEdit}) => {
  return (
    <FormGroup row>
      <Col xs="12">
        <Label className="control-label">Experiment Name</Label>
        <Input
          disabled={isEdit}
          value={name}
          onChange={onNameChange}
          placeholder="Add a name for this Experiment"
        />
      </Col>
    </FormGroup>
  );
};
ExperimentName.propTypes = {
  name: PropTypes.string,
  onNameChange: PropTypes.func,
  isEdit: PropTypes.bool
};

const ExperimentDescription = ({description, onDescriptionChange}) => {
  return (
    <FormGroup row>
      <Col xs="12">
        <Label className="control-label">Description</Label>
        <Input
          type="textarea"
          value={description}
          placeholder="Add a description for this Experiment"
          onChange={onDescriptionChange}
        />
      </Col>
    </FormGroup>
  );
};
ExperimentDescription.propTypes = {
  description: PropTypes.string,
  onDescriptionChange: PropTypes.func
};

const ExperimentOutcome = ({outcome, columns, onOutcomeChange, isEdit}) => {
  return (
    <FormGroup row>
      <Col xs="12">
        <Label className="control-label">Set Outcome for this Experiment </Label>
        <Input
          disabled={isEdit}
          type="select"
          value={outcome}
          onChange={onOutcomeChange}
        >
          <option key="default">Select an Outcome </option>
          {
            columns.map((column, i) => (
              <option key={i}>{column}</option>
            ))
          }
        </Input>
      </Col>
    </FormGroup>
  );
};
ExperimentOutcome.propTypes = {
  outcome: PropTypes.string,
  columns: PropTypes.arrayOf(PropTypes.object),
  onOutcomeChange: PropTypes.func,
  isEdit: PropTypes.bool
};

const CreateExperimentBtn = ({state, createExperiment}) => {
  const isAddExperimentBtnEnabled = () => {
    return state.name.length && state.description.length && state.outcome.length;
  };
  const renderBtnContent = () => {
    if (state.loading) {
      return <IconSVG name="icon-spinner" className="fa-spin" />;
    }
    return state.isEdit ? 'Edit Experiment' : 'Create Experiment';
  };
  return (
    <button
      className="btn btn-primary"
      disabled={!isAddExperimentBtnEnabled()}
      onClick={createExperiment}
    >
      {renderBtnContent()}
    </button>
  );
};
CreateExperimentBtn.propTypes = {
  state: PropTypes.object,
  createExperiment: PropTypes.func
};

const NewExperimentPopoverWrapper = ({popover}) => {
  if (popover !== 'experiment') {
    return null;
  }
  return (
    <div className="new-experiment-popover">
      <div className="popover-container">
        <strong className="popover-heading">
          Create a new Experiment
        </strong>
        <ExperimentOutcomeWrapper />
        <hr />
        <ExperimentNameWrapper />
        <ExperiementDescriptionWrapper />
        <ConnectedCreateExperimentBtn />
      </div>
    </div>
  );
};
NewExperimentPopoverWrapper.propTypes = {
  popover: PropTypes.string
};
const mapDispatchToCreateExperimentBtnProps = () => ({ createExperiment });
const mapStateToCreateExperimentBtnProps = (state) => ({ state: {...state.experiments_create} });
const mapStateToNameProps = (state) => ({ name: state.experiments_create.name, isEdit: state.experiments_create.isEdit });
const mapDispatchToNameProps = () => ({ onNameChange: onExperimentNameChange });
const mapStateToDescriptionProps = (state) => ({ description: state.experiments_create.description });
const mapDispatchToDescriptionToProps = () => ({ onDescriptionChange: onExperimentDescriptionChange });
const mapStateToOutcomeProps = (state) => ({ outcome: state.experiments_create.outcome, columns: state.model_create.columns, isEdit: state.experiments_create.isEdit });
const mapDispatchToOutcomeProps = () => ({onOutcomeChange: onExperimentOutcomeChange});
const mapNEPWStateToProps = (state) => ({ popover: state.experiments_create.popover });

const ExperiementDescriptionWrapper = connect(mapStateToDescriptionProps, mapDispatchToDescriptionToProps)(ExperimentDescription);
const ExperimentNameWrapper = connect(mapStateToNameProps, mapDispatchToNameProps)(ExperimentName);
const ExperimentOutcomeWrapper = connect(mapStateToOutcomeProps, mapDispatchToOutcomeProps)(ExperimentOutcome);
const ConnectedNewExperimentPopoverWrapper = connect(mapNEPWStateToProps)(NewExperimentPopoverWrapper);
const ConnectedCreateExperimentBtn = connect(mapStateToCreateExperimentBtnProps, mapDispatchToCreateExperimentBtnProps)(CreateExperimentBtn);

export default ConnectedNewExperimentPopoverWrapper;
