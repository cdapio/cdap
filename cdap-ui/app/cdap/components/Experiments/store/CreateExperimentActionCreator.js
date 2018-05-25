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

import createExperimentStore, {ACTIONS as CREATEEXPERIMENTACTIONS, POPOVER_TYPES} from 'components/Experiments/store/createExperimentStore';
import experimentDetailStore, {ACTIONS as EXPERIMENTDETAILACTIONS} from 'components/Experiments/store/experimentDetailStore';
import {setAlgorithmsList} from 'components/Experiments/store/SharedActionCreator';
import {myExperimentsApi} from 'api/experiments';
import {getCurrentNamespace} from 'services/NamespaceStore';
import MyDataPrepApi from 'api/dataprep';
import {directiveRequestBodyCreator} from 'components/DataPrep/helper';
import { Observable } from 'rxjs/Observable';
import {NUMBER_TYPES} from 'services/global-constants';

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

function setVisiblePopover(popover = POPOVER_TYPES.EXPERIMENT) {
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
      popover: POPOVER_TYPES.MODEL
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

function updateHyperParam(key, value) {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.UPDATE_HYPER_PARAM,
    payload: {key, value}
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
    }, (err) => {
      setModelCreateError(`Failed to get split details: ${err.response || err}`);
    });
}

function createExperiment() {
  let {model_create, experiments_create} = createExperimentStore.getState();
  let {name, description, outcome, srcpath} = experiments_create;
  let {directives} = model_create;
  let requestBody = directiveRequestBodyCreator(directives);
  let experiment = {
    name,
    description,
    outcome,
    srcpath,
    directives
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
    .subscribe(
      setExperimentCreated.bind(null, experiments_create.name),
      (err) => setExperimentCreateError(`Failed to create experiment: ${err.response || err}`)
    );
}

function pollForSplitStatus(experimentId, modelId) {
  const getStatusOfSplit = (callback, errorCallback) => {
    const params = {
      namespace: getCurrentNamespace(),
      experimentId,
      modelId
    };
    let splitStatusPoll = myExperimentsApi
      .pollModel(params)
      .subscribe(modelDetails => {
        let {status, split} = modelDetails;
        if (status === 'Splitting') {
          return;
        }
        if (status === 'Data Ready' || status === 'Split Failed') {
          splitStatusPoll.unsubscribe();
          return callback(split);
        }
        // TODO: Should this be called on split failed?
        errorCallback();
      });
  };
  return Observable.create((observer) => {
    const successCallback = (split) => {
      observer.next(split);
    };
    const failureCallback = () => {
      observer.error(`Couldn't create split`);
    };
    getStatusOfSplit(successCallback, failureCallback);
  });
}

/*
  This is needed when user clicks "Split Data" button in the split step

  1. Create a split under the model (POST `/models/:modelId/split`)
  2. This will create a split and assign it to the model
  3. Once the model is assigned a split poll the model for an update on its status
  4. Once the status is "Data Ready" or "Split Failed" update UI appropriately.

*/
function createSplitAndUpdateStatus() {
  let {model_create, experiments_create} = createExperimentStore.getState();
  let {directives, schema, modelId} = model_create;
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
    .createSplit({
      namespace: getCurrentNamespace(),
      experimentId: experiments_create.name,
      modelId
    }, splitInfo)
    .mergeMap(() => {
      return pollForSplitStatus(experiments_create.name, modelId);
    })
    .subscribe(
      setSplitDetails.bind(null, experiments_create.name),
      (err) => setModelCreateError(`Failed to split: ${err.response || err}`),
      () => console.log('Split Task complete ', arguments)
    );
}

function createModel() {
  let {experiments_create, model_create} = createExperimentStore.getState();
  let {directives} = model_create;
  let model = {
    name: model_create.name,
    description: model_create.description,
    directives
  };

  return myExperimentsApi
    .createModelInExperiment({
      namespace: getCurrentNamespace(),
      experimentId: experiments_create.name
    }, model)
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
        setModelCreateError(`Failed to create model: ${err.response || err}`);
        setExperimentLoading(false);
      });
}

function trainModel() {
  let {experiments_create, model_create} = createExperimentStore.getState();
  let {name: experimentId} = experiments_create;
  let {modelId} = model_create;
  let postBody = {
    algorithm: model_create.algorithm.name,
    hyperparameters: model_create.algorithm.hyperparameters
  };
  return myExperimentsApi
    .trainModel({
      namespace: getCurrentNamespace(),
      experimentId,
      modelId
    }, postBody)
    .subscribe(() => {
      experimentDetailStore.dispatch({
        type: EXPERIMENTDETAILACTIONS.SET_NEWLY_TRAINING_MODEL,
        payload: {model: model_create}
      });
      createExperimentStore.dispatch({
        type: CREATEEXPERIMENTACTIONS.SET_MODEL_TRAINED
      });
    }, (err) => {
      setModelCreateError(`Failed to train model: ${err.response || err}`);
    });
}

function createWorkspace(filePath) {
    let params = {
      namespace: getCurrentNamespace(),
      path: filePath,
      lines: 10000,
      sampler: 'first',
      scope: 'mmds'
    };

    let headers = {
      'Content-Type': 'text/plain' // FIXME: THIS IS A HACK. NEED TO GET THIS FROM EXPERIMENT
    };

    return MyDataPrepApi.readFile(params, null, headers);
}

function applyDirectives(workspaceId, directives) {
  return MyDataPrepApi
    .getWorkspace({
      namespace: getCurrentNamespace(),
      workspaceId
    })
    .mergeMap(res => {
      let workspaceInfo = res.values[0];
      let params = {
        workspaceId,
        namespace: getCurrentNamespace()
      };
      let requestBody = directiveRequestBodyCreator(directives);
      requestBody.properties = workspaceInfo.properties;
      return MyDataPrepApi.execute(params, requestBody);
    });
}

/*
  This is needed when user has already created an experiment but is still in dataprep stage.
  On refresh in this state we need to go back to the same view the user left.

  1. Fetch the experiments details
  2. Get directives and srcpath from the details
  3. Create a workspace with the srcpath
  4. Once created execute it with the list of directives
  5. Once the workspace is setup and update the stores.

  After step5 CreateView will pass the workspaceid to DataPrepConnection which will render the browser.
*/
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
      return createWorkspace(exp.srcpath);
    })
    .mergeMap(res => {
      let workspaceId = res.values[0].id;
      experiment.workspaceId = workspaceId;
      return applyDirectives(workspaceId, experiment.directives);
    })
    .mergeMap(() => {
    // Get schema with workspaceId and directives
      let {directives} = experiment;
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
    }, (err) => {
      // The error message returned from backend for this request is at err.response.message instead of just err.response
      setExperimentCreateError(`Failed to retrieve experiment: ${err.response.message || err.response || err}`);
    });
};

/*
  This is needed when the user has created the model but still is in the split stage.
  On refresh in this state we need to go back to the same view the user left.

  1. Fetch the experiments details
  2. Get directives and srcpath from the details
  3. Create a workspace with the srcpath
  4. Once created execute it with the list of directives
  5. Once the workspace is setup and update the stores.
  6. Check if the model already has a splitid
  7. If yes fetch split details and update the store

  After step 7 UI will land in the split stage with all the split details.
*/
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
      return createWorkspace(exp.srcpath);
    })
    .mergeMap(res => {
      let workspaceId = res.values[0].id;
      experiment.workspaceId = workspaceId;
      return applyDirectives(workspaceId, experiment.directives);
    })
    .mergeMap(() => {
    // Get schema with workspaceId and directives
      let {directives} = experiment;
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
          pollForSplitStatus(experiment.name, modelId)
            .subscribe(setSplitDetails.bind(null, experiment.name));
        }
      },
      (err) => {
        setExperimentCreateError(`Failed to retrieve experiment and model: ${err.response || err}`);
      }
    );
};

function setAlgorithmList() {
  let {experiments_create} = createExperimentStore.getState();
  let outcome = experiments_create.outcome;
  let {model_create} = createExperimentStore.getState();
  let {directives, algorithmsList} = model_create;
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
        type: CREATEEXPERIMENTACTIONS.SET_VALID_ALGORITHMS_LIST,
        payload: {
          validAlgorithmsList: (NUMBER_TYPES.indexOf(outcomeType) !== -1) ?
            algorithmsList.filter(algo => algo.type === 'REGRESSION')
          :
            algorithmsList.filter(algo => algo.type === 'CLASSIFICATION')
        }
      });
    }, (err) => {
      setExperimentCreateError(`Failed to find algorithms for outcome: ${err.response || err}`);
    });
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

function fetchAlgorithmsList() {
  myExperimentsApi
    .getAlgorithms({
      namespace: getCurrentNamespace()
    })
    .subscribe((algorithmsList) => {
      algorithmsList = algorithmsList.map(alg => {
        return {
          ...alg,
          name: alg.algorithm
        };
      });
      createExperimentStore.dispatch({
        type: CREATEEXPERIMENTACTIONS.SET_ALGORITHMS_LIST,
        payload: {
          algorithmsList
        }
      });
    }, (err) => {
      setExperimentCreateError(`Failed to fetch algorithms: ${err.response || err}`);
    });
}

function setExperimentCreateError(error = null) {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_EXPERIMENT_ERROR,
    payload: {
      error
    }
  });
}

function setModelCreateError(error = null) {
  createExperimentStore.dispatch({
    type: CREATEEXPERIMENTACTIONS.SET_MODEL_ERROR,
    payload: {
      error
    }
  });
}

function setAlgorithmsListForCreateView() {
  setAlgorithmsList()
    .subscribe(
      () => {},
      (err) => {
        setExperimentCreateError(`Failed to get list of algorithms: ${err.response || err}`);
      }
    );
}

export {
  onExperimentNameChange,
  onExperimentDescriptionChange,
  onExperimentOutcomeChange,
  setSrcPath,
  setOutcomeColumns,
  setDirectives,
  setVisiblePopover,
  setExperimentCreated,
  setExperimentLoading,
  onModelNameChange,
  onModelDescriptionChange,
  setModelAlgorithm,
  setWorkspace,
  updateSchema,
  setSplitDetails,
  createExperiment,
  pollForSplitStatus,
  createSplitAndUpdateStatus,
  createModel,
  trainModel,
  getExperimentForEdit,
  getExperimentModelSplitForCreate,
  setAlgorithmList,
  setSplitFinalized,
  resetCreateExperimentsStore,
  fetchAlgorithmsList,
  updateHyperParam,
  setExperimentCreateError,
  setModelCreateError,
  setAlgorithmsListForCreateView
};
