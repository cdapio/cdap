/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
    status: null,
    previewId: null,
    previewData: false,
    macros: {},
    userRuntimeArguments: {},
    // `runtimeArgsForDisplay` combines `macros` map and `userRuntimeArguments` map
    // to create an object that can be used as a prop to the KeyValuePairs component
    runtimeArgsForDisplay: {},
    timeoutInMinutes: 2
  };
};

var preview = (state = getInitialState(), action = {}) => {
  switch(action.type) {
    case previewActions.TOGGLE_PREVIEW_MODE:
      let isPreviewModeEnabled = action.payload.isPreviewModeEnabled;
      return Object.assign({}, state, {isPreviewModeEnabled});
    case previewActions.SET_PREVIEW_START_TIME:
      let startTime = action.payload.startTime;
      return Object.assign({}, state, {startTime});
    case previewActions.SET_PREVIEW_STATUS:
      let status = action.payload.status;
      return Object.assign({}, state, {status});
    case previewActions.SET_PREVIEW_ID:
      let previewId = action.payload.previewId;
      return Object.assign({}, state, {previewId});
    case previewActions.SET_MACROS:
      let macros = action.payload.macrosMap;
      return Object.assign({}, state, {macros});
    case previewActions.SET_USER_RUNTIME_ARGUMENTS:
      let userRuntimeArguments = action.payload.userRuntimeArgumentsMap;
      return Object.assign({}, state, {userRuntimeArguments});
    case previewActions.SET_RUNTIME_ARGS_FOR_DISPLAY:
      let runtimeArgsForDisplay = action.payload.args;
      return Object.assign({}, state, {runtimeArgsForDisplay});
    case previewActions.SET_TIMEOUT_IN_MINUTES:
      let timeoutInMinutes = action.payload.timeoutInMinutes;
      return Object.assign({}, state, {timeoutInMinutes});
    case previewActions.SET_PREVIEW_DATA:
      return Object.assign({}, state, {previewData: true});
    case previewActions.RESET_PREVIEW_DATA:
      return Object.assign({}, state, {previewData: false});
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

angular.module(`${PKG.name}.feature.hydrator`)
  .constant('PREVIEWSTORE_ACTIONS', {
    'TOGGLE_PREVIEW_MODE': 'TOGGLE_PREVIEW_MODE',
    'SET_PREVIEW_START_TIME': 'SET_PREVIEW_START_TIME',
    'SET_PREVIEW_STATUS': 'SET_PREVIEW_STATUS',
    'SET_PREVIEW_ID': 'SET_PREVIEW_ID',
    'PREVIEW_RESET': 'PREVIEW_RESET',
    'SET_MACROS': 'SET_MACROS',
    'SET_USER_RUNTIME_ARGUMENTS': 'SET_USER_RUNTIME_ARGUMENTS',
    'SET_RUNTIME_ARGS_FOR_DISPLAY': 'SET_RUNTIME_ARGS_FOR_DISPLAY',
    'SET_TIMEOUT_IN_MINUTES': 'SET_TIMEOUT_IN_MINUTES',
    'SET_PREVIEW_DATA': 'SET_PREVIEW_DATA',
    'RESET_PREVIEW_DATA': 'RESET_PREVIEW_DATA'
  })
  .factory('HydratorPlusPlusPreviewStore', PreviewStore);
