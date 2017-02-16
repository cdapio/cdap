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

import {combineReducers, createStore} from 'redux';
import WranglerActions from 'components/Wrangler/store/WranglerActions';

const defaultAction = {
  action : '',
  payload : {}
};

const defaultInitialState = {
  initialized: false,
  workspaceId: '',
  data: [],
  headers: [],
  directives: []
};

const wrangler = (state = defaultInitialState, action = defaultAction) => {
  let stateCopy;

  switch (action.type) {
    case WranglerActions.setData:
      stateCopy = Object.assign({}, state, {
        data: action.payload.data,
        headers: action.payload.headers
      });
      break;
    case WranglerActions.setDirectives:
      stateCopy = Object.assign({}, state, {
        data: action.payload.data,
        headers: action.payload.headers,
        directives: action.payload.directives
      });
      break;
    case WranglerActions.setWorkspace:
      stateCopy = Object.assign({}, state, {
        workspaceId: action.payload.workspaceId,
        headers: action.payload.headers || [],
        directives: [],
        data: action.payload.data || [],
        initialized: true
      });
      break;
    case WranglerActions.setInitialized:
      stateCopy = Object.assign({}, state, { initialized: true });
      break;
    case WranglerActions.reset:
      return defaultInitialState;
    default:
      return state;
  }

  return Object.assign({}, state, stateCopy);
};

const WranglerStore = createStore(
  combineReducers({
    wrangler
  }),
  defaultInitialState,
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default WranglerStore;
