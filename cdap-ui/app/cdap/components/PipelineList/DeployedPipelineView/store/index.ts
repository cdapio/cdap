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

enum SORT_ORDER {
  asc = 'asc',
  desc = 'desc',
}

interface IState {
  pipelines: IPipeline[];
  pipelinesLoading: boolean;
  statusMap: IStatusMap;
  runsCountMap: IRunsCountMap;
  deleteError?: string;
  sortColumn: string;
  sortOrder: SORT_ORDER;
  search: string;
  currentPage: number;
  pageLimit: number;
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
  setSort: 'DEPLOYED_PIPELINE_SET_SORT',
  setPage: 'DEPLOYED_PIPELINE_SET_PAGE',
  reset: 'DEPLOYED_PIPELINE_RESET',
};

const defaultInitialState: IState = {
  pipelines: [],
  pipelinesLoading: true,
  statusMap: {},
  runsCountMap: {},
  deleteError: null,
  sortColumn: 'name',
  sortOrder: SORT_ORDER.asc,
  search: '',
  currentPage: 1,
  pageLimit: 25,
};

const deployed: Reducer<IState> = (state = defaultInitialState, action: IAction) => {
  switch (action.type) {
    case Actions.setPipeline:
      return {
        ...state,
        pipelines: action.payload.pipelines,
        sortColumn: 'name',
        sortOrder: SORT_ORDER.asc,
        pipelinesLoading: false,
        deleteError: null,
        currentPage: 1,
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
    case Actions.setSort:
      return {
        ...state,
        sortColumn: action.payload.sortColumn,
        sortOrder: action.payload.sortOrder,
        pipelines: action.payload.pipelines,
        currentPage: 1,
      };
    case Actions.setPage:
      return {
        ...state,
        currentPage: action.payload.currentPage,
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
export { Actions, SORT_ORDER };
