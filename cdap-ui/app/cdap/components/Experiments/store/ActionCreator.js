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

import experimentsStore, {ACTIONS} from 'components/Experiments/store';
import experimentDetailsStore, {ACTIONS as EXPERIMENTDETAILACTIONS} from 'components/Experiments/store/experimentDetailStore';
import {myExperimentsApi} from 'api/experiments';
import {getCurrentNamespace} from 'services/NamespaceStore';
import AlgorithmsListStore, {ACTIONS as AlgorithmsStoreActions} from 'components/Experiments/store/AlgorithmsListStore';

function setExperimentsLoading() {
  experimentsStore.dispatch({
    type: ACTIONS.SET_EXPERIMENTS_LOADING
  });
}

function getExperimentsList() {
  setExperimentsLoading();
  myExperimentsApi
    .list({namespace: getCurrentNamespace()})
    .subscribe(experiments => {
      experiments.forEach(experiment => getModelsListInExperiment(experiment.name));
      experimentsStore.dispatch({
        type: ACTIONS.SET_EXPERIMENTS_LIST,
        payload: {
          experiments
        }
      });
    }, (err) => {
      console.log(err);
    });
}

function getModelsListInExperiment(experimentId) {
  myExperimentsApi
    .getModelsInExperiment({
      experimentId,
      namespace: getCurrentNamespace()
    })
    .subscribe(models => {
      experimentsStore.dispatch({
        type: ACTIONS.SET_MODELS_IN_EXPERIMENT,
        payload: {
          experimentId,
          models
        }
      });
    });
}

function deleteExperiment(experimentId) {
  return myExperimentsApi
    .deleteExperiment({
      namespace: getCurrentNamespace(),
      experimentId
    });
}

function getExperimentDetails(experimentId) {
  getModelsInExperiment(experimentId);
  getSplitsInExperiment(experimentId);
  myExperimentsApi
    .getExperiment({
      namespace: getCurrentNamespace(),
      experimentId
    })
    .subscribe(res => {
      experimentDetailsStore.dispatch({
        type: EXPERIMENTDETAILACTIONS.SET_EXPERIMENT_DETAILS,
        payload: {
          experimentDetails: {
            ...res
          }
        }
      });
    });
}

function getModelsInExperiment(experimentId) {
  experimentDetailsStore.dispatch({
    type: EXPERIMENTDETAILACTIONS.SET_LOADING
  });
  myExperimentsApi.getModelsInExperiment({
    namespace: getCurrentNamespace(),
    experimentId
  })
  .subscribe(models => {
    experimentDetailsStore.dispatch({
      type: EXPERIMENTDETAILACTIONS.SET_MODELS,
      payload: {
        models
      }
    });
    models.forEach(model => getModelStatus(experimentId, model.id));
  });
}

function getSplitsInExperiment(experimentId) {
  myExperimentsApi
    .getSplitsInExperiment({
      namespace: getCurrentNamespace(),
      experimentId
    })
    .subscribe(splits => {
      experimentDetailsStore.dispatch({
        type: EXPERIMENTDETAILACTIONS.SET_SPLITS,
        payload: {
          splits
        }
      });
    });
}

function getModelStatus(experimentId, modelId) {
  myExperimentsApi
    .getModelStatus({
      namespace: getCurrentNamespace(),
      experimentId,
      modelId
    })
    .subscribe(modelStatus => {
      experimentDetailsStore.dispatch({
        type: EXPERIMENTDETAILACTIONS.SET_MODEL_STATUS,
        payload: {
          modelId,
          modelStatus
        }
      });
    });
}

function setActiveModel(activeModelId) {
  experimentDetailsStore.dispatch({
    type: EXPERIMENTDETAILACTIONS.SET_ACTIVE_MODEL,
    payload: {
      activeModelId
    }
  });
}

const getAlgorithmLabel = (algorithm) => {
  let algorithmsList = AlgorithmsListStore.getState();
  let match = algorithmsList.find(algo => algo.name === algorithm);
  if (match) {
    return match.label;
  }
  return algorithm;
};

const getHyperParamLabel = (algorithm, hyperparam) => {
  let algorithmsList = AlgorithmsListStore.getState();
  let match = algorithmsList.find(algo => algo.name === algorithm);
  if (match) {
    let matchingHyperParameter = match.hyperparameters.find(hp => hp.name === hyperparam);
    if (matchingHyperParameter) {
      return matchingHyperParameter.label;
    }
    return hyperparam;
  }
  return hyperparam;
};

const setAlgorithmsList = () => {
  let algoList = AlgorithmsListStore.getState();
  if (algoList.length) {
    return;
  }
  myExperimentsApi
    .getAlgorithms({
      namespace: getCurrentNamespace()
    })
    .subscribe(algorithmsList => {
      algorithmsList = algorithmsList.map(algo => ({...algo, name: algo.algorithm}));
      AlgorithmsListStore.dispatch({
        type: AlgorithmsStoreActions.SET_ALGORITHMS_LIST,
        payload: {algorithmsList}
      });
    });
};

export {
  setExperimentsLoading,
  getExperimentsList,
  getModelsListInExperiment,
  deleteExperiment,
  getExperimentDetails,
  getModelsInExperiment,
  getSplitsInExperiment,
  getModelStatus,
  setActiveModel,
  getAlgorithmLabel,
  getHyperParamLabel,
  setAlgorithmsList
};

