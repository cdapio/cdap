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
import {
  setModelAlgorithm,
  trainModel,
  setAlgorithmList,
  updateHyperParam,
  setModelCreateError
} from 'components/Experiments/store/CreateExperimentActionCreator';
import {getAlgorithmLabel, getHyperParamLabel} from 'components/Experiments/store/SharedActionCreator';
import {Label} from 'reactstrap';
import {objectQuery} from 'services/helpers';
import HyperParamWidget from 'components/Experiments/CreateView/MLAlgorithmSelection/HyperParamWidget';
import startCase from 'lodash/startCase';
import classnames from 'classnames';
import Alert from 'components/Alert';

require('./MLAlgorithmSelection.scss');

const MLAlgorithmsList = ({algorithmsList, setModelAlgorithm, selectedAlgorithm, error}) => {
  if (!objectQuery(algorithmsList, 'length')) {
    return null;
  }
  const getSelectedAlgorithm = (algorithm) => {
    let hyperparameters = {};
    algorithm.hyperparameters.forEach(hp => {
      hyperparameters[hp.name] = hp.defaultVal;
    });
    return {
      name: algorithm.name,
      hyperparameters
    };
  };
  return (
    <div className="ml-algorithm-list-view">
      <div className="ml-algorithm-category-title">Algorithm Type: {startCase(algorithmsList[0].type || '')}</div>
      {
        algorithmsList.map((algo, i) => {
          return (
            <div
              key={i}
              className={classnames("ml-algorithm-list-item", {
                selected: selectedAlgorithm.name === algo.name
              })}
              onClick={setModelAlgorithm.bind(null, getSelectedAlgorithm(algo))}
            >
              <input
                type="radio"
                name="algo-radio"
                value={algo.name}
                checked={selectedAlgorithm.name === algo.name}
                onChange={setModelAlgorithm.bind(null, getSelectedAlgorithm(algo))}
              />
              <Label className="control-label">
                {algo.label}
              </Label>
            </div>
          );
        })
      }
      <ConnectedAddModelBtn />
      {
        error ?
          <Alert
            message={error}
            type='error'
            showAlert={true}
            onClose={setModelCreateError}
          />
        :
          null
      }
    </div>
  );
};
MLAlgorithmsList.propTypes = {
  algorithmsList: PropTypes.arrayOf(PropTypes.object),
  setModelAlgorithm: PropTypes.func,
  selectedAlgorithm: PropTypes.object,
  error: PropTypes.any
};
const getComponent = (hyperParam) => {
  /*
    Example of hyper parameters from backend
    {
      "type": "int|double",
      "defaultVal": "32",
      "validValues": [],
      "range": {
        "min": "2",
        "max": "1000",
        "isMinInclusive": true
      }
    },
    {
      "type": "string",
      "defaultVal": "gaussian",
      "validValues": [
        "gaussian",
        "binomial",
        "poisson",
        "gamma"
      ]
    },
    {
      "type": "bool",
      "defaultVal": "true",
      "validValues": [
        "true",
        "false"
      ]
    },
  */
  const BACKEND_PROPS_TO_REACT_PROPS = {
    'int': {
      'defaultVal': 'defaultValue',
      'range,min': 'min',
      'range,max': 'max',
      'name': 'name'
    },
    'double': {
      'defaultVal': 'defaultValue',
      'range,min': 'min',
      'range,max': 'max',
      'name': 'name'
    },
    'bool': {
      'defaultVal': 'defaultValue',
      'validValues': 'options',
      'name': 'name'
    },
    'string': {
      'defaultVal': 'defaultValue',
      'validValues': 'options',
      'name': 'name'
    }
  };
  let matchedType = BACKEND_PROPS_TO_REACT_PROPS[hyperParam.type];
  if (!matchedType) {
    return {};
  }
  let config = {};
  Object
    .keys(matchedType)
    .forEach(hyperParamProp => {
      if (hyperParamProp.indexOf(',') !== -1) {
        config[matchedType[hyperParamProp]] = objectQuery.apply(null, [hyperParam, ...hyperParamProp.split(',')]);
      } else {
        config[matchedType[hyperParamProp]] = hyperParam[hyperParamProp];
      }
    });
  return config;
};
// Will be used post demo.
const MLAlgorithmDetails = ({algorithm, algorithmsList}) => {
  if (!algorithm.name.length) {
    return null;
  }
  return (
    <div className="ml-algorithm-hyper-parameters-wrapper">
      <strong> Configure hyperparameters for {getAlgorithmLabel(algorithm.name)} </strong>
      <div className="ml-algorithm-hyper-parameters">
        {
          algorithmsList
            .find(al => al.name === algorithm.name)
            .hyperparameters
            .map(hyperParam => {
              let actualValue = algorithm.hyperparameters[hyperParam.name];
              let config = {
                ...getComponent(hyperParam),
                label: getHyperParamLabel(algorithm.name, hyperParam.name),
                value: actualValue
              };
              return (
                <HyperParamWidget
                  type={hyperParam.type}
                  config={config}
                  onChange={(e) => {
                    // FIX: as we use non-input elements this doesn't need be an "event" object
                    updateHyperParam(hyperParam.name, e.target.value);
                  }}
                />
              );
            })
        }
      </div>
    </div>
  );
};
MLAlgorithmDetails.propTypes = {
  algorithm: PropTypes.object,
  algorithmsList: PropTypes.arrayOf(PropTypes.object)
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
  algorithmsList: state.model_create.validAlgorithmsList,
  selectedAlgorithm: state.model_create.algorithm,
  error: state.model_create.error
});
const mapDispatchToMLAlgorithmsListProps = () => ({setModelAlgorithm});
const mapStateToMLAlgorithmDetailsProps = (state) => ({
  algorithmsList: state.model_create.algorithmsList,
  algorithm: state.model_create.algorithm
});

const ConnectedMLAlgorithmsList = connect(mapStateToMLAlgorithmsListProps, mapDispatchToMLAlgorithmsListProps)(MLAlgorithmsList);
const ConnectedMLAlgorithmDetails = connect(mapStateToMLAlgorithmDetailsProps)(MLAlgorithmDetails);
const ConnectedAddModelBtn = connect(mapStateToAddModelBtnProps, mapDispatchToAddModelBtnProps)(AddModelBtn);

export default function MLAlgorithmSelection() {
  setAlgorithmList();
  return (
    <Provider store={createExperimentStore}>
      <div className="ml-algorithm-selection">
        <h3>Select a Machine Learning Algorithm</h3>
        <div className="ml-algorithm-list-details">
          <ConnectedMLAlgorithmsList />
          <ConnectedMLAlgorithmDetails />
          <br />
        </div>
      </div>
    </Provider>
  );
}
