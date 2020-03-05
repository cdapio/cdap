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
import { connect, Provider } from 'react-redux';
import createExperimentStore, {
  CREATION_STEPS,
} from 'components/Experiments/store/createExperimentStore';
import { overrideCreationStep } from 'components/Experiments/store/CreateExperimentActionCreator';
import classnames from 'classnames';
import isNil from 'lodash/isNil';
import T from 'i18n-react';

const PREFIX = 'features.Experiments.CreateView';

require('./ExperimentMetadata.scss');

const ExperimentMetadataWrapper = ({
  modelName,
  modelDescription,
  directives,
  algorithm,
  active_step,
}) => {
  let isAlgorithmEmpty = () => isNil(algorithm) || !algorithm.length;
  return (
    <div className="experiment-metadata">
      <div>
        <strong>{T.translate(`${PREFIX}.modelNameWithColon`)}</strong>
        <span>{modelName}</span>
        <div className="model-description-wrapper">
          <strong>{T.translate(`${PREFIX}.modelDescriptionWithColon`)}</strong>
          <p className="model-description">{modelDescription}</p>
        </div>
      </div>
      <div>
        <strong>{T.translate(`${PREFIX}.numDirectives`)}</strong>
        <span>{directives.length}</span>
        {directives.length ? (
          <div
            className="btn btn-link"
            onClick={overrideCreationStep.bind(null, CREATION_STEPS.DATAPREP)}
          >
            {T.translate('commons.edit')}
          </div>
        ) : null}
      </div>
      <div>
        <strong>{T.translate(`${PREFIX}.splitMethod`)}</strong>
        <span>{T.translate(`${PREFIX}.random`)}</span>
        {active_step === CREATION_STEPS.ALGORITHM_SELECTION ? (
          <div
            className="btn btn-link"
            onClick={overrideCreationStep.bind(null, CREATION_STEPS.DATASPLIT)}
          >
            {T.translate('commons.edit')}
          </div>
        ) : null}
      </div>
      <div
        className={classnames({
          grayed: isAlgorithmEmpty(),
        })}
      >
        <strong>{T.translate(`${PREFIX}.MLAlgorithm`)}</strong>
        <span>{isAlgorithmEmpty() ? '--' : algorithm}</span>
      </div>
    </div>
  );
};
ExperimentMetadataWrapper.propTypes = {
  modelName: PropTypes.string,
  modelDescription: PropTypes.string,
  directives: PropTypes.array,
  algorithm: PropTypes.string,
  active_step: PropTypes.string,
};
const mapStateToProps = (state) => ({
  modelName: state.model_create.name,
  modelDescription: state.model_create.description,
  directives: state.model_create.directives,
  active_step: state.active_step.step_name,
  algorithm: !state.model_create.algorithm.name.length
    ? ''
    : state.model_create.validAlgorithmsList.find(
        (algo) => algo.name === state.model_create.algorithm.name
      ).label,
});
const ConnectedExperimentMetadata = connect(mapStateToProps, null)(ExperimentMetadataWrapper);

export default function ExperimentMetadata() {
  return (
    <Provider store={createExperimentStore}>
      <ConnectedExperimentMetadata />
    </Provider>
  );
}
