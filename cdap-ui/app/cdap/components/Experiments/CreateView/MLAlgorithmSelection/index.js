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
import {Provider, connect} from 'react-redux';
import createExperimentStore from 'components/Experiments/store/createExperimentStore';
import {setModelAlgorithm, trainModel, setAlgorithmList} from 'components/Experiments/store/ActionCreator';
import {Label} from 'reactstrap';

require('./MLAlgorithmSelection.scss');

const MLAlgorithmsList = ({algorithmsList, setModelAlgorithm, selectedAlgorithm}) => {
  return (
    <div className="ml-algorithm-list-view">
      <h3>Select a Machine Learning Algorithm</h3>
      <div className="ml-algorithm-category-title">Continous</div>
      {
        algorithmsList.map((algo, i) => {
          return (
            <div
              key={i}
              className="ml-algorithm-list-item"
              onClick={setModelAlgorithm.bind(null, algo)}
            >
              <input
                type="radio"
                name="algo-radio"
                value={algo.name}
                checked={selectedAlgorithm.name === algo.name}
                onChange={setModelAlgorithm.bind(null, algo)}
              />
              <Label className="control-label">
                {algo.label}
              </Label>
            </div>
          );
        })
      }
    </div>
  );
};
MLAlgorithmsList.propTypes = {
  algorithmsList: PropTypes.arrayOf(PropTypes.object),
  setModelAlgorithm: PropTypes.func,
  selectedAlgorithm: PropTypes.object
};
// Will be used post demo.
const MLAlgorithmDetails = ({algorithm}) => {
  if (!algorithm.name.length) {
    return null;
  }
  return (
    <pre>
      {JSON.stringify(algorithm, null, 2)}
    </pre>
  );
};
MLAlgorithmDetails.propTypes = {
  algorithm: PropTypes.object
};
const AddModelBtn = ({algorithm, trainModel}) => {
  return (
    <button
      className="btn btn-primary"
      disabled={!algorithm.name.length}
      onClick={trainModel}
    >
      Train Model
    </button>
  );
};
AddModelBtn.propTypes = {
  algorithm: PropTypes.object,
  trainModel: PropTypes.func
};

const mapStateToAddModelBtnProps = (state) => ({ algorithm: state.model_create.algorithm});
const mapDispatchToAddModelBtnProps = () => ({trainModel});
const mapStateToMLAlgorithmsListProps = (state) => ({
  algorithmsList: state.model_create.algorithmsList,
  selectedAlgorithm: state.model_create.algorithm
});
const mapDispatchToMLAlgorithmsListProps = () => ({setModelAlgorithm});
// const mapStateToMLAlgorithmDetailsProps = (state) => ({ algorithm: state.model_create.algorithm });

const ConnectedMLAlgorithmsList = connect(mapStateToMLAlgorithmsListProps, mapDispatchToMLAlgorithmsListProps)(MLAlgorithmsList);
// const ConnectedMLAlgorithmDetails = connect(mapStateToMLAlgorithmDetailsProps)(MLAlgorithmDetails);
const ConnectedAddModelBtn = connect(mapStateToAddModelBtnProps, mapDispatchToAddModelBtnProps)(AddModelBtn);


export default function MLAlgorithmSelection() {
  setAlgorithmList();
  return (
    <Provider store={createExperimentStore}>
      <div className="ml-algorithm-selection">
        <div className="ml-algorithm-list-details">
          <ConnectedMLAlgorithmsList />
          {/* <ConnectedMLAlgorithmDetails /> */}
          <br />
        </div>
        <ConnectedAddModelBtn />
      </div>
    </Provider>
  );
}
