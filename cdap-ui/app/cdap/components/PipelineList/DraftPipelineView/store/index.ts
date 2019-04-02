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
import { IDraft } from 'components/PipelineList/DraftPipelineView/types';
import { Reducer, Store as StoreInterface } from 'redux';
import { IAction } from 'services/redux-helpers';

enum SORT_ORDER {
  asc = 'asc',
  desc = 'desc',
}

interface IState {
  list: IDraft[];
  sortColumn: string;
  sortOrder: SORT_ORDER;
  currentPage: number;
  pageLimit: number;
}

interface IStore {
  drafts: IState;
}

const Actions = {
  setDrafts: 'SET_PIPELINE_DRAFTS',
  setSort: 'SET_PIPELINE_DRAFTS_SORT',
  setPage: 'SET_PIPELINE_DRAFTS_PAGE',
  reset: 'DRAFTS_RESET',
};

const defaultInitialState: IState = {
  list: [],
  sortColumn: 'name',
  sortOrder: SORT_ORDER.asc,
  currentPage: 1,
  pageLimit: 25,
};

const drafts: Reducer<IState> = (state = defaultInitialState, action: IAction) => {
  switch (action.type) {
    case Actions.setDrafts:
      return {
        ...state,
        list: action.payload.list,
        currentPage: 1,
      };
    case Actions.setSort:
      return {
        ...state,
        sortColumn: action.payload.sortColumn,
        sortOrder: action.payload.sortOrder,
        list: action.payload.list,
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
    drafts,
  }),
  {
    drafts: defaultInitialState,
  },
  composeEnhancers('PipelineDraftsEnhancers')()
);

export default Store;
export { Actions, SORT_ORDER };
