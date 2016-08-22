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
    isPreviewModeEnabled: false,
    startTime: null,
    previewId: null
  };
};

var preview = (state = getInitialState(), action = {}) => {
  let stateCopy;
  switch(action.type) {
    case previewActions.TOGGLE_PREVIEW_MODE:
      stateCopy = Object.assign({}, state);
      const {isPreviewModeEnabled} = action.payload;
      stateCopy.isPreviewModeEnabled = isPreviewModeEnabled;
      return Object.assign({}, state, stateCopy);
    case previewActions.SET_PREVIEW_START_TIME:
      stateCopy = Object.assign({}, state);
      const {startTime} = action.payload;
      stateCopy.startTime = startTime;
      return Object.assign({}, state, stateCopy);
    case previewActions.SET_PREVIEW_ID:
      stateCopy = Object.assign({}, state);
      const {previewId} = action.payload;
      stateCopy.previewId = previewId;
      return Object.assign({}, state, stateCopy);
    case previewActions.PREVIEW_RESET:
      return getInitialState();
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
    'SET_PREVIEW_START_TIME': 'SET_PREVIEW_START_TIME',
    'SET_PREVIEW_ID': 'SET_PREVIEW_ID',
    'PREVIEW_RESET': 'PREVIEW_RESET'
  })
  .factory('HydratorPlusPlusPreviewStore', PreviewStore);
