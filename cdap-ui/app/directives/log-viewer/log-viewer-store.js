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

var LogViewerStore = (LOGVIEWERSTORE_ACTIONS, Redux, ReduxThunk) => {
  const startTime = (state = new Date(), action = {}) => {
    switch(action.type) {
      case LOGVIEWERSTORE_ACTIONS.START_TIME:
        if(!action.payload.startTime) {
          return state;
        }
        return action.payload.startTime;
      default:
        return state;
    }
  };

  //Scroll Position Reducer
  const scrollPosition = (state = Date.now(), action = {}) => {
    switch(action.type) {
      case LOGVIEWERSTORE_ACTIONS.SCROLL_POSITION:
        if(!action.payload.scrollPosition) {
          return state;
        }
        return action.payload.scrollPosition;
      default:
        return state;
    }
  };

  //Combine the reducers
  let {combineReducers, applyMiddleware} = Redux;
  let combinedReducers = combineReducers({
    startTime,
    scrollPosition
  });
  let getInitialState = () => {
    return {
      startTime: Date.now(),
      scrollPosition: Date.now()
    };
  };

  return Redux.createStore(
    combinedReducers,
    getInitialState(),
    Redux.compose(
      applyMiddleware(ReduxThunk.default)
    )
  );
};

LogViewerStore.$inject = ['LOGVIEWERSTORE_ACTIONS', 'Redux', 'ReduxThunk'];

angular.module(`${PKG.name}.commons`)
  .constant('LOGVIEWERSTORE_ACTIONS', {
    'START_TIME' : 'START_TIME',
    'SCROLL_POSITION' : 'SCROLL_POSITION'
  })
  .factory('LogViewerStore', LogViewerStore);
