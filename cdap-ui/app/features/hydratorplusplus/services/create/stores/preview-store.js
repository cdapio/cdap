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

let previewActions;
let getInitialState = () => {
  return {
    isPreviewModeEnabled: false
  };
};

var preview = (state = getInitialState(), action = {}) => {
  switch(action.type) {
    case previewActions.TOGGLE_PREVIEW_MODE:
      let stateCopy = Object.assign({}, state);
      stateCopy.isPreviewModeEnabled = action.payload;

      return Object.assign({}, state, stateCopy);
    default:
      return state;
  }
};

var PreviewStore = (PREVIEWSTORE_ACTIONS, Redux, ReduxThunk) => {
  previewActions = PREVIEWSTORE_ACTIONS;
  let {combineReducers, applyMiddleware} = Redux;

  let combineReducer = combineReducers({
    preview
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

angular.module(`${PKG.name}.feature.hydratorplusplus`)
  .constant('PREVIEWSTORE_ACTIONS', {
    'TOGGLE_PREVIEW_MODE': 'TOGGLE_PREVIEW_MODE',
  })
  .factory('HydratorPlusPlusPreviewStore', PreviewStore);
