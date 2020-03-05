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
import { connect } from 'react-redux';
import { FormGroup, Label, Col, Input } from 'reactstrap';
import {
  onExperimentNameChange,
  onExperimentDescriptionChange,
  onExperimentOutcomeChange,
  createExperiment,
} from 'components/Experiments/store/CreateExperimentActionCreator';
import { POPOVER_TYPES } from 'components/Experiments/store/createExperimentStore';

import IconSVG from 'components/IconSVG';
import T from 'i18n-react';

const PREFIX = 'features.Experiments.CreateView';

const ExperimentName = ({ name, onNameChange }) => {
  return (
    <FormGroup row>
      <Col xs="12">
        <Label className="control-label">{T.translate(`${PREFIX}.experimentName`)}</Label>
        <Input
          value={name}
          onChange={onNameChange}
          placeholder={T.translate(`${PREFIX}.experimentNamePlaceholder`)}
        />
      </Col>
    </FormGroup>
  );
};
ExperimentName.propTypes = {
  name: PropTypes.string,
  onNameChange: PropTypes.func,
};

const ExperimentDescription = ({ description, onDescriptionChange }) => {
  return (
    <FormGroup row>
      <Col xs="12">
        <Label className="control-label">{T.translate('commons.descriptionLabel')}</Label>
        <Input
          type="textarea"
          value={description}
          placeholder={T.translate(`${PREFIX}.descriptionPlaceholder`)}
          onChange={onDescriptionChange}
        />
      </Col>
    </FormGroup>
  );
};
ExperimentDescription.propTypes = {
  description: PropTypes.string,
  onDescriptionChange: PropTypes.func,
};

const ExperimentOutcome = ({ outcome, columns, onOutcomeChange }) => {
  return (
    <FormGroup row>
      <Col xs="12">
        <Label className="control-label">{T.translate(`${PREFIX}.setOutcome`)}</Label>
        <Input type="select" value={outcome} onChange={onOutcomeChange}>
          <option key="default">{T.translate(`${PREFIX}.selectOutcome`)}</option>
          {columns.map((column, i) => (
            <option key={i}>{column}</option>
          ))}
        </Input>
      </Col>
    </FormGroup>
  );
};
ExperimentOutcome.propTypes = {
  outcome: PropTypes.string,
  columns: PropTypes.arrayOf(PropTypes.object),
  onOutcomeChange: PropTypes.func,
};

const CreateExperimentBtn = ({ state, createExperiment }) => {
  const isAddExperimentBtnEnabled = () => {
    return state.name.length && state.description.length && state.outcome.length;
  };
  const renderBtnContent = () => {
    if (state.loading) {
      return <IconSVG name="icon-spinner" className="fa-spin" />;
    }
    return T.translate(`${PREFIX}.createExperiment`);
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
  createExperiment: PropTypes.func,
};

const NewExperimentPopoverWrapper = ({ popover, isEdit }) => {
  if (popover !== POPOVER_TYPES.EXPERIMENT || isEdit) {
    return null;
  }
  return (
    <div className="new-experiment-popover">
      <div className="popover-container">
        <strong className="popover-heading">{T.translate(`${PREFIX}.createNewExperiment`)}</strong>
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
  popover: PropTypes.string,
  isEdit: PropTypes.bool,
};
const mapDispatchToCreateExperimentBtnProps = () => ({ createExperiment });
const mapStateToCreateExperimentBtnProps = (state) => ({ state: { ...state.experiments_create } });
const mapStateToNameProps = (state) => ({ name: state.experiments_create.name });
const mapDispatchToNameProps = () => ({ onNameChange: onExperimentNameChange });
const mapStateToDescriptionProps = (state) => ({
  description: state.experiments_create.description,
});
const mapDispatchToDescriptionToProps = () => ({
  onDescriptionChange: onExperimentDescriptionChange,
});
const mapStateToOutcomeProps = (state) => ({
  outcome: state.experiments_create.outcome,
  columns: state.model_create.columns,
});
const mapDispatchToOutcomeProps = () => ({ onOutcomeChange: onExperimentOutcomeChange });
const mapNEPWStateToProps = (state) => ({
  popover: state.experiments_create.popover,
  isEdit: state.experiments_create.isEdit,
});

const ExperiementDescriptionWrapper = connect(
  mapStateToDescriptionProps,
  mapDispatchToDescriptionToProps
)(ExperimentDescription);
const ExperimentNameWrapper = connect(mapStateToNameProps, mapDispatchToNameProps)(ExperimentName);
const ExperimentOutcomeWrapper = connect(
  mapStateToOutcomeProps,
  mapDispatchToOutcomeProps
)(ExperimentOutcome);
const ConnectedNewExperimentPopoverWrapper = connect(mapNEPWStateToProps)(
  NewExperimentPopoverWrapper
);
const ConnectedCreateExperimentBtn = connect(
  mapStateToCreateExperimentBtnProps,
  mapDispatchToCreateExperimentBtnProps
)(CreateExperimentBtn);

export default ConnectedNewExperimentPopoverWrapper;
