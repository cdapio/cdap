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

import { combineReducers, createStore } from 'redux';
import { composeEnhancers } from 'services/helpers';
import {
  IPipeline,
  IStatusMap,
  IRunsCountMap,
} from 'components/PipelineList/DeployedPipelineView/types';
import { Reducer, Store as StoreInterface } from 'redux';
import { IAction } from 'services/redux-helpers';
import { Action } from 'rxjs/scheduler/Action';

interface IState {
  pipelines: IPipeline[];
  pipelinesLoading: boolean;
  statusMap: IStatusMap;
  runsCountMap: IRunsCountMap;
  deleteError?: string;
  search: string;
}

interface IStore {
  deployed: IState;
}

const Actions = {
  setPipeline: 'DEPLOYED_PIPELINE_SET_LIST',
  setStatusMap: 'DEPLOYED_PIPELINE_SET_STATUS_MAP',
  setRunsCountMap: 'DEPLOYED_PIPELINE_SET_RUNS_COUNT_MAP',
  setSearch: 'DEPLOYED_SET_SEARCH',
  setDeleteError: 'DEPLOYED_PIPELINE_SET_DELETE_ERROR',
  clearDeleteError: 'DEPLOYED_PIPELINE_CLEAR_DELETE_ERROR',
  reset: 'DEPLOYED_PIPELINE_RESET',
};

const defaultInitialState: IState = {
  pipelines: [],
  pipelinesLoading: true,
  statusMap: {},
  runsCountMap: {},
  deleteError: null,
  search: '',
};

const deployed: Reducer<IState> = (state = defaultInitialState, action: IAction) => {
  switch (action.type) {
    case Actions.setPipeline:
      return {
        ...state,
        pipelines: action.payload.pipelines,
        pipelinesLoading: false,
        deleteError: null,
      };
    case Actions.setStatusMap:
      return {
        ...state,
        statusMap: action.payload.statusMap,
      };
    case Actions.setRunsCountMap:
      return {
        ...state,
        runsCountMap: action.payload.runsCountMap,
      };
    case Actions.setDeleteError:
      return {
        ...state,
        deleteError: action.payload.deleteError,
      };
    case Actions.clearDeleteError:
      return {
        ...state,
        deleteError: null,
      };
    case Actions.setSearch:
      return {
        ...state,
        search: action.payload.search,
      };
    case Actions.reset:
      return defaultInitialState;
    default:
      return state;
  }
};

const Store: StoreInterface<IStore> = createStore(
  combineReducers({
    deployed,
  }),
  {
    deployed: defaultInitialState,
  },
  composeEnhancers('DeployedPipelineStore')()
);

export default Store;
export { Actions };
