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
  SET_EXPERIMENTS_LIST: 'SET_EXPERIMENTS_LIST',
  SET_EXPERIMENTS_LOADING: 'SET_EXPERIMENTS_LOADING',
  SET_MODELS_IN_EXPERIMENT: 'SET_MODELS_IN_EXPERIMENT'
};

const DEFAULT_EXPERIMENTS = {
  list: [],
  loading: false
};

const experiments = (state = DEFAULT_EXPERIMENTS, action = defaultAction) => {
  switch (action.type) {
    case ACTIONS.SET_EXPERIMENTS_LIST:
      return {
        ...state,
        list: action.payload.experiments,
        loading: false
      };
    case ACTIONS.SET_EXPERIMENTS_LOADING:
      return {
        ...state,
        loading: true
      };
    case ACTIONS.SET_MODELS_IN_EXPERIMENT:
      return {
        ...state,
        list: state.list.map(experiment => {
          if (experiment.name === action.payload.experimentId) {
            return Object.assign({}, experiment, {models: action.payload.models});
          }
          return experiment;
        })
      };
    default:
      return state;
  }
};

const store = createStore(
  combineReducers({experiments}),
  {
    experiments: DEFAULT_EXPERIMENTS
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default store;
export {ACTIONS};
