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

import { createStore, combineReducers, applyMiddleware } from 'redux';
import { defaultAction, composeEnhancers } from 'services/helpers';
import thunk from 'redux-thunk';

const AVAILABLE_PLUGINS_ACTIONS = {
  setPluginsMap: 'SET_AVAILABLE_PLUGINS_MAP',
  reset: 'AVAILABLE_PLUGINS_RESET',
};

let getInitialState = () => {
  return {
    pluginsMap: {},
  };
};

const plugins = (state = getInitialState(), action = defaultAction) => {
  switch (action.type) {
    case AVAILABLE_PLUGINS_ACTIONS.setPluginsMap:
      return Object.assign({}, state, {
        pluginsMap: action.payload.pluginsMap,
      });
    case AVAILABLE_PLUGINS_ACTIONS.reset:
      return getInitialState();
    default:
      return state;
  }
};

const AvailablePluginsStore = createStore(
  combineReducers({
    plugins,
  }),
  getInitialState(),
  composeEnhancers('AvailablePluginsStore')(applyMiddleware(thunk))
);

export default AvailablePluginsStore;
export { AVAILABLE_PLUGINS_ACTIONS };
