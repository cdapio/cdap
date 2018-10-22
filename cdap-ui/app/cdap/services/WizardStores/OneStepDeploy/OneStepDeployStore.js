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

import { combineReducers, createStore } from 'redux';
import OneStepDeployActions from 'services/WizardStores/OneStepDeploy/OneStepDeployActions';

const defaultAction = {
  type: '',
  payload: {},
};
const defaultState = {
  steps: [],
  __complete: false,
  __skipped: false,
  __error: false,
};
const defaultInitialState = {
  oneStepDeploy: defaultState,
};

const oneStepDeploy = (state = defaultState, action = defaultAction) => {
  let stateCopy;

  switch (action.type) {
    case OneStepDeployActions.setName:
      stateCopy = Object.assign({}, state, {
        name: action.payload,
      });
      break;
    case OneStepDeployActions.onReset:
      return defaultState;
    default:
      return state;
  }
  return Object.assign({}, stateCopy, {
    __complete: true,
  });
};

const OneStepDeployStoreWrapper = () => {
  return createStore(
    combineReducers({
      oneStepDeploy,
    }),
    defaultInitialState
  );
};

const OneStepDeployStore = OneStepDeployStoreWrapper();
export default OneStepDeployStore;
