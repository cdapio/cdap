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

import {createStore, combineReducers} from 'redux';
import {defaultAction, composeEnhancers} from 'services/helpers';
import {REGRESSION_ALGORITHMS} from 'components/Experiments/store/MLAlgorithmsList';

const ACTIONS = {
  SET_EXPERIMENT_NAME: 'SET_EXPERIMENT_NAME',
  SET_EXPERIMENT_DESCRIPTION: 'SET_EXPERIMENT_DESCRIPTION',
  SET_EXPERIMENT_OUTCOME: 'SET_EXPERIMENT_OUTCOME',
  SET_EXPERIMENT_SRC_PATH: 'SET_EXPERIMENT_SRC_PATH',
  SET_CREATE_EXPERIMENT_LOADING: 'SET_CREATE_EXPERIMENT_LOADING',
  SET_EXPERIMENT_METADATA_FOR_EDIT: 'SET_EXPERIMENT_METADATA_FOR_EDIT',
  SET_VISIBLE_POPOVER: 'SET_VISIBLE_POPOVER',

  SET_SPLIT_INFO: 'SET_SPLIT_INFO',
  SET_SCHEMA: 'SET_SCHEMA',
  SET_OUTCOME_COLUMNS: 'SET_OUTCOME_COLUMNS',
  SET_DIRECTIVES: 'SET_DIRECTIVES',
  SET_MODEL_NAME: 'SET_MODEL_NAME',
  SET_MODEL_ID: 'SET_MODEL_ID',
  SET_EXPERIMENT_MODEL_FOR_EDIT: 'SET_EXPERIMENT_MODEL_FOR_EDIT',
  SET_MODEL_DESCRIPTION: 'SET_MODEL_DESCRIPTION',
  SET_MODEL_ML_ALGORITHM: 'SET_MODEL_ML_ALGORITHM',
  SET_ALGORITHMS_LIST: 'SET_ALGORITHMS_LIST',
  SET_WORKSPACE_ID: 'SET_WORKSPACE_ID',
  SET_SPLIT_FINALIZED: 'SET_SPLIT_FINALIZED',
  RESET: 'RESET'
};

const DEFAULT_EXPERIMENTS_CREATE_VALUE = {
  name: '',
  description: '',
  outcome: '',
  srcpath: '',
  loading: false,
  popover: 'experiment',
  isEdit: false,
  workspaceId: null
};

const DEFAULT_MODEL_CREATE_VALUE = {
  name: '',
  description: '',
  modelId: null,

  directives: [],
  columns: [],
  schema: null,

  splitInfo: {},
  isSplitFinalized: false,

  algorithm: {
    name: ''
  },

  algorithmsList: REGRESSION_ALGORITHMS,
};

const experiments_create = (state = DEFAULT_EXPERIMENTS_CREATE_VALUE, action = defaultAction) => {
  switch (action.type) {
    case ACTIONS.SET_EXPERIMENT_NAME:
      return {
        ...state,
        name: action.payload.name
      };
    case ACTIONS.SET_EXPERIMENT_DESCRIPTION:
      return {
        ...state,
        description: action.payload.description
      };
    case ACTIONS.SET_EXPERIMENT_OUTCOME:
      return {
        ...state,
        outcome: action.payload.outcome
      };
    case ACTIONS.SET_EXPERIMENT_SRC_PATH:
      return {
        ...state,
        srcpath: action.payload.srcpath
      };
    case ACTIONS.SET_CREATE_EXPERIMENT_LOADING:
      return {
        ...state,
        loading: action.payload.loading
      };
    case ACTIONS.SET_WORKSPACE_ID:
      return {
        ...state,
        workspaceId: action.payload.workspaceId
      };
    case ACTIONS.SET_EXPERIMENT_METADATA_FOR_EDIT:
    case ACTIONS.SET_EXPERIMENT_MODEL_FOR_EDIT: {
      let {name, description, outcome, srcpath, workspaceId} = action.payload.experimentDetails;
      return {
        ...state,
        name, description, outcome, srcpath, workspaceId,
        isEdit: true,
        loading: false
      };
    }
    case ACTIONS.SET_VISIBLE_POPOVER:
      return {
        ...state,
        popover: action.payload.popover
      };
    case ACTIONS.RESET:
      return DEFAULT_EXPERIMENTS_CREATE_VALUE;
    default:
      return state;
  }
};
const model_create = (state = DEFAULT_MODEL_CREATE_VALUE, action = defaultAction) => {
  switch (action.type) {
    case ACTIONS.SET_MODEL_ID:
      return {
        ...state,
        modelId: action.payload.modelId
      };
    case ACTIONS.SET_MODEL_NAME:
      return {
        ...state,
        name: action.payload.name
      };
    case ACTIONS.SET_MODEL_DESCRIPTION:
      return {
        ...state,
        description: action.payload.description
      };
    case ACTIONS.SET_SPLIT_INFO:
      return {
        ...state,
        splitInfo: action.payload.splitInfo
      };
    case ACTIONS.SET_SPLIT_FINALIZED:
      return {
        ...state,
        isSplitFinalized: action.payload.isSplitFinalized
      };
    case ACTIONS.SET_OUTCOME_COLUMNS:
      return {
        ...state,
        columns: action.payload.columns
      };
    case ACTIONS.SET_DIRECTIVES:
      return {
        ...state,
        directives: action.payload.directives
      };
    case ACTIONS.SET_MODEL_ML_ALGORITHM:
      return {
        ...state,
        algorithm: action.payload.algorithm
      };
    case ACTIONS.SET_EXPERIMENT_METADATA_FOR_EDIT:
    case ACTIONS.SET_SCHEMA:
      return {
        ...state,
        schema: action.payload.schema || state.schema
      };
    case ACTIONS.SET_EXPERIMENT_MODEL_FOR_EDIT: {
      let {name, description, id: modelId} = action.payload.modelDetails;
      return {
        ...state,
        name,
        description,
        modelId,
        splitInfo: action.payload.splitInfo
      };
    }
    case ACTIONS.SET_ALGORITHMS_LIST:
      return {
        ...state,
        algorithmsList: action.payload.algorithmsList
      };
    case ACTIONS.RESET:
      return DEFAULT_MODEL_CREATE_VALUE;
    default:
      return state;
  }
};

const createExperimentStore = createStore(
  combineReducers({
    experiments_create,
    model_create
  }),
  {
    experiments_create: DEFAULT_EXPERIMENTS_CREATE_VALUE,
    model_create: DEFAULT_MODEL_CREATE_VALUE
  },
  composeEnhancers('CreateExperimentStore')()
);

export {ACTIONS};
export default createExperimentStore;
