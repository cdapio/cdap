/*
 * Copyright © 2020 Cask Data, Inc.
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

import { combineReducers, createStore, Store as InterfaceStore } from 'redux';
import SystemDelayActions from './SystemDelayActions';
import { composeEnhancers } from 'services/helpers';
import { IAction } from 'services/redux-helpers';
import DataSource from 'services/datasource';

interface ISystemDelayStoreState {
  activeDataSources: DataSource[];
  showDelay: boolean;
}

const defaultAction = {
  type: '',
  payload: [],
};

const defaultInitialState = {
  activeDataSources: [],
};

const activeDataSources = (state = [], action: IAction = defaultAction) => {
  switch (action.type) {
    case SystemDelayActions.registerDataSource: {
      return [...state, action.payload];
    }
    default:
      return state;
  }
};

const showDelay = (state = false, action: IAction = defaultAction) => {
  switch (action.type) {
    case SystemDelayActions.showDelay:
      return true;
    case SystemDelayActions.hideDelay:
      return false;
    default:
      return state;
  }
};

/**
 * Store to manage system delay globally across CDAP UI.
 */
const SystemDelayStore: InterfaceStore<ISystemDelayStoreState> = createStore(
  combineReducers({
    activeDataSources,
    showDelay,
  }),
  defaultInitialState,
  composeEnhancers('SystemDelayStore')()
);

export default SystemDelayStore;
