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

/*
  {
    pluginsWidgetMap: {
      <artifact-name>-<artifact-version>-<artifact-scope>: {
        //widget-json-for-plugins
        "widgets.<plugin-name>-<plugin-type>": "" // stringified json.
      },
      <artifact-name>-<artifact-version>-<artifact-scope>: {
        //widget-json-for-plugins
        "widgets.<plugin-name>-<plugin-type>": "" // stringified json.
      },
      <artifact-name>-<artifact-version>-<artifact-scope>: {
        //widget-json-for-plugins
        "widgets.<plugin-name>-<plugin-type>": "" // stringified json.
      }
    }
  }

*/

var _PLUGINS_WIDGETS_ACTIONS;

var pluginsWidgetMap = (state = {}, action = {}) => {
  switch(action.type) {
    case _PLUGINS_WIDGETS_ACTIONS.WIDGET_JSON_FETCH:
      {
        let {key, res} = action.payload;
        let resObj = {};
        angular.forEach(res, function(value, key) {
          if (key.indexOf('widgets.') !== -1) {
            resObj[key] = value;
          }
        });
        return Object.assign({}, state, {[key]: resObj});
      }
    case _PLUGINS_WIDGETS_ACTIONS.WIDGET_JSON_FETCH_START:
      {
        let {key} = action.payload;
        return Object.assign({}, state, {[key]: {}});
      }
    case _PLUGINS_WIDGETS_ACTIONS.RESET:
      return {};
    default:
      return state;
  }
};

var HydratorPlusPlusPluginWidgetsStore = (PLUGINS_WIDGETS_ACTIONS, Redux, ReduxThunk) => {
  _PLUGINS_WIDGETS_ACTIONS = PLUGINS_WIDGETS_ACTIONS;
  let {combineReducers, applyMiddleware} = Redux;

  let combineReducer = combineReducers({
    pluginsWidgetMap
  });

  return Redux.createStore(
    combineReducer,
    {},
    Redux.compose(
      applyMiddleware(ReduxThunk.default),
      window.devToolsExtension ? window.devToolsExtension() : f => f
    )
  );
};
HydratorPlusPlusPluginWidgetsStore.$inject = ['PLUGINS_WIDGETS_ACTIONS', 'Redux', 'ReduxThunk'];

angular.module(PKG.name + '.feature.hydratorplusplus')
  .constant('PLUGINS_WIDGETS_ACTIONS', {
    'WIDGET_JSON_FETCH_START': 'WIDGET_JSON_FETCH_START',
    'WIDGET_JSON_FETCH': 'WIDGET_JSON_FETCH',
    'RESET': 'WIDGET_JSON_RESET'
  })
  .factory('HydratorPlusPlusPluginWidgetsStore', HydratorPlusPlusPluginWidgetsStore);
