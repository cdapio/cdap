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
let pluginsActions;
let getInitialState = () => {
  return {
    pluginsMap: {}
  };
};


const plugins = (state = getInitialState(), action = {}) => {
  switch (action.type) {
    case pluginsActions.setPluginsMap:
      return Object.assign({}, state, {
        pluginsMap: action.payload.pluginsMap
      });
    case pluginsActions.reset:
      return getInitialState();
    default:
      return state;
  }
};

var AvailablePluginsStore = (AVAILABLE_PLUGINS_ACTIONS, Redux, ReduxThunk) => {
  pluginsActions = AVAILABLE_PLUGINS_ACTIONS;
  let {combineReducers, applyMiddleware} = Redux;

  let combineReducer = combineReducers({
    plugins
  });

  return Redux.createStore(
    combineReducer,
    getInitialState(),
    Redux.compose(
      applyMiddleware(ReduxThunk.default),
      window.devToolsExtension ? window.devToolsExtension() : f => f
    )
  );
};

angular.module(`${PKG.name}.feature.hydrator`)
  .constant('AVAILABLE_PLUGINS_ACTIONS', {
    setPluginsMap: 'SET_AVAILABLE_PLUGINS_MAP',
    reset: 'AVAILABLE_PLUGINS_RESET'
  })
  .factory('AvailablePluginsStore', AvailablePluginsStore);
