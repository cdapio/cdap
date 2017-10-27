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

import {createStore, combineReducers} from 'redux';
import {defaultAction} from 'services/helpers';

const ACTIONS = {
  SET_EXPERIMENT_NAME: 'SET_EXPERIMENT_NAME',
  SET_EXPERIMENT_DESCRIPTION: 'SET_EXPERIMENT_DESCRIPTION',
  SET_EXPERIMENT_OUTCOME: 'SET_EXPERIMENT_OUTCOME',
  SET_EXPERIMENT_SRC_PATH: 'SET_EXPERIMENT_SRC_PATH',
  SET_NEW_EXPERIMENT_CREATED: 'SET_NEW_EXPERIMENT_CREATED',
  SET_CREATE_EXPERIMENT_LOADING: 'SET_CREATE_EXPERIMENT_LOADING',

  SET_OUTCOME_COLUMNS: 'SET_OUTCOME_COLUMNS',
  SET_DIRECTIVES: 'SET_DIRECTIVES',
  SET_MODEL_NAME: 'SET_MODEL_NAME',
  SET_MODEL_DESCRIPTION: 'SET_MODEL_DESCRIPTION',
  SET_MODEL_CREATED: 'SET_MODEL_CREATED',
  SET_MODEL_ML_ALGORITHM: 'SET_MODEL_ML_ALGORITHM',
  SET_WORKSPACE_ID: 'SET_WORKSPACE_ID'
};

const DEFAULT_EXPERIMENTS_CREATE_VALUE = {
  name: '',
  description: '',
  outcome: '',
  srcpath: '',
  loading: false,
  isExperimentCreated: false
};

const DEFAULT_MODEL_CREATE_VALUE = {
  name: '',
  description: '',
  directives: [],
  columns: [],
  workspaceId: '',
  splitMethod: 'random',
  algorithm: {
    name: ''
  },
  algorithmsList: [
    {
      name: 'linear.regression',
      label: 'Linear Regression'
    },
    {
      name: 'generalized.linear.regression',
      label: 'Generalized Linear Regression'
    },
    {
      name: 'decision.tree.regression',
      label: 'Decision Tree Regression'
    },
    {
      name: 'random.forest.regression',
      label: 'Random Forest Regression'
    },
    {
      name: 'gradient.boosted.tree.regression',
      label: 'Gradient Boosted Tree Regression'
    }
  ],
  isModelCreated: false
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
    case ACTIONS.SET_NEW_EXPERIMENT_CREATED:
      return {
        ...state,
        isExperimentCreated: action.payload.isExperimentCreated
      };
    case ACTIONS.SET_CREATE_EXPERIMENT_LOADING:
      return {
        ...state,
        loading: action.payload.loading
      };
    default:
      return state;
  }
};
const model_create = (state = DEFAULT_MODEL_CREATE_VALUE, action = defaultAction) => {
  switch (action.type) {
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
    case ACTIONS.SET_MODEL_CREATED:
      return {
        ...state,
        isModelCreated: true
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
    case ACTIONS.SET_WORKSPACE_ID:
      return {
        ...state,
        workspaceId: action.payload.workspaceId
      };
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
    experiments_create: DEFAULT_EXPERIMENTS_CREATE_VALUE
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export {ACTIONS};
export default createExperimentStore;
