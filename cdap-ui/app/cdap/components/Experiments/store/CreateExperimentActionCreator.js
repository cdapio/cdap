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
        if (status === 'Complete' || status === 'Failed') {
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
    .subscribe(
      setSplitDetails.bind(null, experiments_create.name),
      (err) => console.log('Splitting Failed: ', err),
      () => console.log('Split Task complete ', arguments)
    );
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
    });
}

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
    .subscribe(
      () => {},
      (err) => console.log('Adding split faied: ', err)
    );
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
    });
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
  addSplitToModel,
  setSplitFinalized,
  resetCreateExperimentsStore,
  fetchAlgorithmsList,
  updateHyperParam
};
