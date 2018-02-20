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
  let {offset, limit} = experimentsStore.getState().experiments;
  myExperimentsApi
    .list({ namespace: getCurrentNamespace(), offset, limit })
    .subscribe(res => {
      let experiments = res.experiments;
      let totalCount = res.totalRowCount;
      experiments.forEach(experiment => getModelsListInExperiment(experiment.name));
      experimentsStore.dispatch({
        type: ACTIONS.SET_EXPERIMENTS_LIST,
        payload: {
          experiments,
          totalCount
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
    .subscribe(res => {
      let models = res.models;
      let modelsCount = res.totalRowCount;
      experimentsStore.dispatch({
        type: ACTIONS.SET_MODELS_IN_EXPERIMENT,
        payload: {
          experimentId,
          models,
          modelsCount
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
  let { modelsOffset: offset, modelsLimit: limit } = experimentDetailsStore.getState();
  let ModelsObservable$ = myExperimentsApi.getModelsInExperiment({
    namespace: getCurrentNamespace(),
    experimentId,
    offset,
    limit
  });

  ModelsObservable$.subscribe(res => {
    let models = res.models;
    experimentDetailsStore.dispatch({
      type: EXPERIMENTDETAILACTIONS.SET_MODELS,
      payload: {
        models,
        totalCount: res.totalRowCount
      }
    });
    getSplitsInExperiment(experimentId);
  });
  return ModelsObservable$;
}

function handleModelsPageChange({ selected }) {
  let { modelsLimit, name: experimentId } = experimentDetailsStore.getState();
  experimentDetailsStore.dispatch({
    type: EXPERIMENTDETAILACTIONS.SET_MODEL_PAGINATION,
    payload: {
      modelsOffset: selected * modelsLimit
    }
  });
  updateQueryStringWithModelsOffset();
  getExperimentDetails(experimentId);
}

function updatePaginationForModels({ modelsLimit, modelsOffset }) {
  experimentDetailsStore.dispatch({
    type: EXPERIMENTDETAILACTIONS.SET_MODEL_PAGINATION,
    payload: {
      modelsOffset,
      modelsLimit
    }
  });
}

function updateQueryStringWithModelsOffset() {
  let { modelsOffset: offset, modelsLimit: limit } = experimentDetailsStore.getState();
  let query = `offset=${offset}&limit=${limit}`;
  let obj = {
    title: document.title,
    url: `${location.pathname}?${query}`
  };
  history.pushState(obj, obj.title, obj.url);
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
  return myExperimentsApi
    .pollModelStatus({
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
  let state = experimentDetailsStore.getState();
  let {splitDetails} = state.models.find(model => model.id === activeModelId);
  if (!splitDetails) {
    getSplitsInExperiment(state.name);
  }
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

function updatePagination({ limit, offset }) {
  experimentsStore.dispatch({
    type: ACTIONS.SET_PAGINATION,
    payload: { limit, offset }
  });
}

function updateQueryString() {
  let {offset, limit} = experimentsStore.getState().experiments;
  let query = `offset=${offset}&limit=${limit}`;
  let obj = {
    title: document.title,
    url: `${location.pathname}?${query}`
  };
  history.pushState(obj, obj.title, obj.url);
}

function handlePageChange({selected}) {
  let {limit} = experimentsStore.getState().experiments;
  experimentsStore.dispatch({
    type: ACTIONS.SET_PAGINATION,
    payload: {
      offset: selected * limit
    }
  });
  updateQueryString();
  getExperimentsList();
}

function resetExperimentDetailStore() {
  experimentDetailsStore.dispatch({
    type: EXPERIMENTDETAILACTIONS.RESET
  });
}

function resetNewlyTrainingModel() {
  experimentDetailsStore.dispatch({
    type: EXPERIMENTDETAILACTIONS.RESET_NEWLY_TRAINING_MODEL
  });
}
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
  setAlgorithmsList,
  updatePaginationForModels,
  handleModelsPageChange,
  handlePageChange,
  updatePagination,
  resetExperimentDetailStore,
  resetNewlyTrainingModel
};

