/*
 * Copyright © 2017 Cask Data, Inc.
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
import createExperimentStore, {ACTIONS as CREATEEXPERIMENTACTIONS} from 'components/Experiments/store/createExperimentStore';
import experimentDetailsStore, {ACTIONS as EXPERIMENTDETAILACTIONS} from 'components/Experiments/store/experimentDetailStore';
import {myExperimentsApi} from 'api/experiments';
import NamespaceStore, {getCurrentNamespace} from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';
import MLAlgorithmsList from 'components/Experiments/store/MLAlgorithmsList';

function setExperimentsLoading() {
  experimentsStore.dispatch({
    type: ACTIONS.SET_EXPERIMENTS_LOADING
  });
}

function getExperimentsList() {
  setExperimentsLoading();
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  myExperimentsApi
    .list({namespace})
    .subscribe(experiments => {
      experiments.forEach(experiment => getModelsListInExperiment(experiment.name));
      experimentsStore.dispatch({
        type: ACTIONS.SET_EXPERIMENTS_LIST,
        payload: {
          experiments
        }
      });
    });
}

function getModelsListInExperiment(experimentId) {
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  myExperimentsApi
    .getModelsInExperiment({experimentId, namespace})
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

function onExperimentNameChange(e) {
  let value = e.target.value;
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_EXPERIMENT_NAME,
    payload: {name: value}
  });
}

function onExperimentDescriptionChange(e) {
  let value = e.target.value;
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_EXPERIMENT_DESCRIPTION,
    payload: {description: value}
  });
}

function onExperimentOutcomeChange(e) {
  let value = e.target.value;
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_EXPERIMENT_OUTCOME,
    payload: {outcome: value}
  });
}

function setSrcPath(srcpath) {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_EXPERIMENT_SRC_PATH,
    payload: {srcpath}
  });
}

function setOutcomeColumns(columns) {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_OUTCOME_COLUMNS,
    payload: {columns}
  });
}

function setDirectives(directives) {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_DIRECTIVES,
    payload: {directives}
  });
}

function setExperimentCreated(value) {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_NEW_EXPERIMENT_CREATED,
    payload: {
      isExperimentCreated: typeof value === 'boolean' ? value : true
    }
  });
}

function setExperimentLoading(value = true) {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_CREATE_EXPERIMENT_LOADING,
    payload: {loading: value}
  });
}

function onModelNameChange(e) {
  let value = e.target.value;
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_MODEL_NAME,
    payload: {name: value}
  });
}

function onModelDescriptionChange(e) {
  let value = e.target.value;
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_MODEL_DESCRIPTION,
    payload: {description: value}
  });
}

function setModelCreated() {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_MODEL_CREATED
  });
}

function setModelAlgorithm(algorithm) {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_MODEL_ML_ALGORITHM,
    payload: {algorithm}
  });
}

function setWorkspace(workspaceId) {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_WORKSPACE_ID,
    payload: {workspaceId}
  });
}

function createExperiment(experiment) {
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  return myExperimentsApi.createExperiment({namespace, experimentId: experiment.name}, experiment);
}

function createModel(experiment, model) {
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  return myExperimentsApi.createModelInExperiment({namespace, experimentId: experiment.name}, model);
}

function createExperimentAndModel() {
  let {experiments_create, model_create} = createExperimentStore.getState();
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  let {workspaceId, directives} = model_create;
  let requestBody = directiveRequestBodyCreator(directives);
  let experiment = {
    name: experiments_create.name,
    description: experiments_create.description,
    outcome: experiments_create.outcome,
    srcpath: experiments_create.srcpath,
    workspaceId: model_create.workspaceId
  };
  setExperimentLoading();

  let model = {
    name: model_create.name,
    description: model_create.description,
    algorithm: model_create.algorithm.name,
    hyperparameters: {},
    split: 'random',
    directives: model_create.directives,
    features: model_create.columns.filter(column => column !== experiments_create.outcome)
  };
  createExperiment(experiment)
    .combineLatest(
      MyDataPrepApi.getSchema({ namespace, workspaceId}, requestBody)
    )
    .flatMap((res) => {
      let tempSchema = {
        name: 'avroSchema',
        type: 'record',
        fields: res[1]
      };
      let splitInfo = {
        schema: tempSchema,
        directives,
        type: 'random',
        parameters: { percent: "80"},
        description: `Default Random split created for model: ${model_create.name}`
      };
      return myExperimentsApi.createSplit({namespace, experimentId: experiments_create.name}, splitInfo);
    })
    .flatMap(({id: split}) => createModel(experiment, {...model, split}))
    .subscribe(() => {
      let {selectedNamespace: namespace} = NamespaceStore.getState();
      window.location.href = `${window.location.origin}/cdap/ns/${namespace}/experiments/${experiment.name}`;
    }, (err) => {
      console.log('ERROR: ', err); // FIXME: We should surface the errors. There will be errors
      setExperimentLoading(false);
    });
}

function deleteExperiment(experimentId) {
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  return myExperimentsApi
    .deleteExperiment({
      namespace,
      experimentId
    });
}

function getExperimentDetails(experimentId) {
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  getModelsInExperiment(experimentId);
  getSplitsInExperiment(experimentId);
  myExperimentsApi
    .getExperiment({
      namespace,
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
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  experimentDetailsStore.dispatch({
    type: EXPERIMENTDETAILACTIONS.SET_LOADING
  });
  myExperimentsApi.getModelsInExperiment({
    namespace,
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
  let {selectedNamespace: namespace} = NamespaceStore.getState();
  myExperimentsApi
    .getSplitsInExperiment({
      namespace,
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
  let match = MLAlgorithmsList.find(algo => algo.name === algorithm);
  if (match) {
    return match.label;
  }
  return algorithm;
};

const getExperimentForEdit = (experimentId) => {
  myExperimentsApi
    .getExperiment({
      namespace: getCurrentNamespace(),
      experimentId
    })
    .subscribe(experimentDetails => {
      createExperimentStore.dispatch({
        type: CREATEEXPERIMENTACTIONS.SET_EXPERIMENT_METADATA_FOR_EDIT,
        payload: {experimentDetails}
      });
    });
};

export {
  setExperimentsLoading,
  getExperimentsList,
  getModelsListInExperiment,
  onExperimentNameChange,
  onExperimentDescriptionChange,
  onExperimentOutcomeChange,
  setExperimentLoading,
  setOutcomeColumns,
  setDirectives,
  setWorkspace,
  setExperimentCreated,
  onModelNameChange,
  onModelDescriptionChange,
  setModelCreated,
  setModelAlgorithm,
  createExperimentAndModel,
  setSrcPath,
  getExperimentDetails,
  getModelsInExperiment,
  setActiveModel,
  deleteExperiment,
  getAlgorithmLabel,
  getModelStatus,
  getExperimentForEdit
};

