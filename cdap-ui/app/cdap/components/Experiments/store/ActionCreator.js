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
import createExperimentStore, {ACTIONS as CREATEEXPERIMENTACTIONS} from 'components/Experiments/store/createExperimentStore';
import experimentDetailsStore, {ACTIONS as EXPERIMENTDETAILACTIONS} from 'components/Experiments/store/experimentDetailStore';
import {myExperimentsApi} from 'api/experiments';
import NamespaceStore, {getCurrentNamespace} from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';
import {CLASSIFIER_ALGORITHMS, REGRESSION_ALGORITHMS} from 'components/Experiments/store/MLAlgorithmsList';
import { Observable } from 'rxjs/Observable';
import {NUMBER_TYPES} from 'services/global-constants';

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
    }, (err) => {
      console.log(err);
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
function setVisiblePopover(popover = 'experiment') {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_VISIBLE_POPOVER,
    payload: {popover}
  });
}
function setExperimentCreated(experimentId) {
  let url = `${location.pathname}?experimentId=${experimentId}`;
  history.replaceState(
    { url },
    document.title,
    url
  );
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_VISIBLE_POPOVER,
    payload: {
      popover: 'model'
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

function updateSchema(fields) {
  let schema = {
    name: 'avroSchema',
    type: 'record',
    fields
  };
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_SCHEMA,
    payload: {schema}
  });
}

function setSplitDetails(experimentId, splitId) {
  if (splitId.id) {
    splitId = splitId.id;
  }
  myExperimentsApi
    .getSplitDetails({
      namespace: getCurrentNamespace(),
      experimentId,
      splitId
    })
    .subscribe((splitInfo) => {
      createExperimentStore.dispatch({
        type: CREATEEXPERIMENTACTIONS.SET_SPLIT_INFO,
        payload: {splitInfo}
      });
    });
}

function createExperiment() {
  let {model_create, experiments_create} = createExperimentStore.getState();
  let {directives} = model_create;
  let requestBody = directiveRequestBodyCreator(directives);
  let experiment = {
    ...experiments_create
  };
  MyDataPrepApi
    .getSchema({
      namespace: getCurrentNamespace(),
      workspaceId: experiments_create.workspaceId
    }, requestBody)
    .mergeMap((schema) => {
      // The outcome will always be simple type. So ["null", "anything"] should give correct outcomeType at the end.
      let outcomeType = schema
        .find(field => field.name === experiment.outcome)
        .type
        .filter(t => t !== 'null')
        .pop();
      experiment.outcomeType = outcomeType;
      updateSchema(schema);
      return myExperimentsApi.createExperiment({
        namespace: getCurrentNamespace(),
        experimentId: experiment.name
      }, experiment);
    })
    .subscribe(setExperimentCreated.bind(null, experiments_create.name));
}

function pollForSplitStatus(experimentId, split) {
  const getStatusOfSplit = (callback, errorCallback) => {
    const params = {
      namespace: getCurrentNamespace(),
      experimentId,
      splitId: split.id
    };
    let splitStautsPoll = myExperimentsApi
      .getSplitStatus(params)
      .subscribe(status => {
        if (status === 'Splitting') {
          return;
        }
        if (status === 'Complete') {
          splitStautsPoll.unsubscribe();
          return callback();
        }
        errorCallback();
      });
  };
  return Observable.create((observer) => {
    const successCallback = () => {
      observer.next(split);
    };
    const failureCallback = () => {
      observer.error(`Couldn't create split`);
    };
    getStatusOfSplit(successCallback, failureCallback);
  });
}

function createSplitAndUpdateStatus() {
  let {model_create, experiments_create} = createExperimentStore.getState();
  let {directives, schema} = model_create;
  let splitInfo = {
    schema,
    directives,
    type: 'random',
    parameters: { percent: "80"},
    description: `Default Random split created for model: ${model_create.name}`
  };
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_SPLIT_INFO,
    payload: {
      splitInfo: {
        id: null,
        status: 'CREATING' // INTERMEDIATE STATE TO SHOW LOADING ANIMATION FOR THE SPLIT BUTTON
      }
    }
  });
  myExperimentsApi
    .createSplit({namespace: getCurrentNamespace(), experimentId: experiments_create.name}, splitInfo)
    .mergeMap(split => {
      setSplitDetails(experiments_create.name, split.id, true);
      addSplitToModel(split.id);
      return pollForSplitStatus(experiments_create.name, split);
    })
    .subscribe(setSplitDetails.bind(null, experiments_create.name));
}

function createModel() {
  let {experiments_create, model_create} = createExperimentStore.getState();
  let {split} = model_create;
  let model = {
    name: model_create.name,
    description: model_create.description
  };

  return myExperimentsApi
    .createModelInExperiment({namespace: getCurrentNamespace(), experimentId: experiments_create.name}, {...model, split})
    .subscribe(({id: modelId}) => {
      createExperimentStore.dispatch({
        type: CREATEEXPERIMENTACTIONS.SET_MODEL_ID,
        payload: {modelId}
      });
      setExperimentLoading(false);
      let url = `${location.pathname}${location.search}&modelId=${modelId}`;
      history.replaceState(
        { url },
        document.title,
        url
      );
    }, (err) => {
      console.log('ERROR: ', err); // FIXME: We should surface the errors. There will be errors
      setExperimentLoading(false);
    });
}

function trainModel() {
  let {experiments_create, model_create} = createExperimentStore.getState();
  let {name: experimentId} = experiments_create;
  let {modelId} = model_create;
  let postBody = {
    algorithm: model_create.algorithm.name,
    hyperParameters: {}
  };
  return myExperimentsApi
    .trainModel({
      namespace: getCurrentNamespace(),
      experimentId,
      modelId
    }, postBody)
    .subscribe(() => {
      window.location.href = `${window.location.origin}/cdap/ns/${getCurrentNamespace()}/experiments/${experimentId}`;
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
  let match = [...REGRESSION_ALGORITHMS, ...CLASSIFIER_ALGORITHMS].find(algo => algo.name === algorithm);
  if (match) {
    return match.label;
  }
  return algorithm;
};

const getExperimentForEdit = (experimentId) => {
  setExperimentLoading();
  let experiment;
  myExperimentsApi
    .getExperiment({
      namespace: getCurrentNamespace(),
      experimentId
    })
    .mergeMap(exp => {
      experiment = exp;
    // Get workspace info for directives
      return MyDataPrepApi.getWorkspace({
        namespace: getCurrentNamespace(),
        workspaceId: exp.workspaceId
      });
    })
    .mergeMap(workspaceInfo => {
    // Get schema with workspaceId and directives
      let directives = workspaceInfo.values[0].recipe.directives;
      setDirectives(directives);
      let requestBody = directiveRequestBodyCreator(directives);
      return MyDataPrepApi.getSchema({
        namespace: getCurrentNamespace(),
        workspaceId: experiment.workspaceId
      }, requestBody);
    })
    .subscribe(fields => {
      let schema = {
        name: 'avroSchema',
        type: 'record',
        fields
      };
      createExperimentStore.dispatch({
        type: CREATEEXPERIMENTACTIONS.SET_EXPERIMENT_METADATA_FOR_EDIT,
        payload: {
          experimentDetails: experiment,
          schema
        }
      });
    });
};

const getExperimentModelSplitForCreate = (experimentId, modelId) => {
  setExperimentLoading();
  let experiment, model;
  // Get experiment
  myExperimentsApi
    .getExperiment({
      namespace: getCurrentNamespace(),
      experimentId
    })
    .mergeMap(exp => {
      experiment = exp;
    // Get workspace info for directives
      return MyDataPrepApi.getWorkspace({
        namespace: getCurrentNamespace(),
        workspaceId: exp.workspaceId
      });
    })
    .mergeMap(workspaceInfo => {
    // Get schema with workspaceId and directives
      let directives = workspaceInfo.values[0].recipe.directives;
      setDirectives(directives);
      let requestBody = directiveRequestBodyCreator(directives);
      return MyDataPrepApi.getSchema({
        namespace: getCurrentNamespace(),
        workspaceId: experiment.workspaceId
      }, requestBody);
    })
    .mergeMap((schema) => {
      updateSchema(schema);
    // Get model details
      return myExperimentsApi
        .getModel({
          namespace: getCurrentNamespace(),
          experimentId: experiment.name,
          modelId
        });
    })
    .mergeMap(m => {
      model = m;
      // The user refreshed before creating the split. So no split info is present in model.
      if (!m.split) {
        return Observable.create((observer) => {
          observer.next(false);
        });
      }
      // If split already created get the split info to check the status of split.
      return myExperimentsApi
        .getSplitDetails({
          namespace: getCurrentNamespace(),
          experimentId: experiment.name,
          splitId: m.split
        });
    })
    .subscribe(
      splitInfo => {
        let payload = {
          experimentDetails: experiment,
          modelDetails: model
        };
        if (splitInfo) {
          payload.splitInfo = splitInfo;
        }
        createExperimentStore.dispatch({
          type: CREATEEXPERIMENTACTIONS.SET_EXPERIMENT_MODEL_FOR_EDIT,
          payload
        });
        if (typeof splitInfo === 'object' && splitInfo.status !== 'Complete') {
          pollForSplitStatus(experiment.name, splitInfo)
            .subscribe(setSplitDetails.bind(null, experiment.name));
        }
      },
      (err) => {
        console.log('Failed to retrieve experiment and model: ', err);
      }
    );
};

function setAlgorithmList() {
  let {experiments_create} = createExperimentStore.getState();
  let outcome = experiments_create.outcome;
  let {model_create} = createExperimentStore.getState();
  let {directives} = model_create;
  let requestBody = directiveRequestBodyCreator(directives);
  MyDataPrepApi
    .getSchema({
      namespace: getCurrentNamespace(),
      workspaceId: experiments_create.workspaceId
    }, requestBody)
    .subscribe(fields => {
      updateSchema(fields);
      let outcomeType = fields
        .find(field => field.name === outcome)
        .type
        .filter(t => t !== 'null')
        .pop();
      createExperimentStore.dispatch({
        type: CREATEEXPERIMENTACTIONS.SET_ALGORITHMS_LIST,
        payload: {
          algorithmsList: (NUMBER_TYPES.indexOf(outcomeType) !== -1) ? REGRESSION_ALGORITHMS : CLASSIFIER_ALGORITHMS
        }
      });
    });
}

function addSplitToModel(splitId) {
  let {experiments_create, model_create} = createExperimentStore.getState();
  myExperimentsApi
    .setSplitToModel({
      namespace: getCurrentNamespace(),
      experimentId: experiments_create.name,
      modelId: model_create.modelId
    }, {id: splitId})
    .subscribe();
}

function setSplitFinalized() {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_SPLIT_FINALIZED,
    payload: {isSplitFinalized: true}
  });
}

function resetCreateExperimentsStore() {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.RESET
  });
}

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
  setModelAlgorithm,
  createExperiment,
  createSplitAndUpdateStatus,
  createModel,
  setSrcPath,
  getExperimentDetails,
  getModelsInExperiment,
  setActiveModel,
  deleteExperiment,
  getAlgorithmLabel,
  getModelStatus,
  getExperimentForEdit,
  addSplitToModel,
  getExperimentModelSplitForCreate,
  setSplitFinalized,
  trainModel,
  resetCreateExperimentsStore,
  setAlgorithmList,
  setVisiblePopover
};

