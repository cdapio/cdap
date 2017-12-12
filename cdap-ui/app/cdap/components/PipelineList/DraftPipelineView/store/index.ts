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

interface IState {
  list: IDraft[];
}

interface IStore {
  drafts: IState;
}

const Actions = {
  setDrafts: 'SET_PIPELINE_DRAFTS',
  reset: 'DRAFTS_RESET',
};

const defaultInitialState: IState = {
  list: [],
};

const drafts: Reducer<IState> = (state = defaultInitialState, action: IAction) => {
  switch (action.type) {
    case Actions.setDrafts:
      return {
        ...state,
        list: action.payload.list,
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
export { Actions };
