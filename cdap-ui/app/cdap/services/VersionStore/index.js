/*
 * Copyright © 2016 Cask Data, Inc.
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
import VersionActions from './VersionActions';

const defaultAction = {
  action : '',
  payload : {}
};

const defaultInitialState = {
  version : ''
};

const version = (state = '', action = defaultAction) => {
  switch(action.type) {
    case VersionActions.updateVersion:
        return action.payload.version;
    default:
      return state;
  }
};

const VersionStore = createStore(
  combineReducers({
    version
  }),
  defaultInitialState,
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default VersionStore;
