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

import { combineReducers, createStore } from 'redux';
import {
  compareByTimestamp,
  getDateID,
  getRequestsByDate,
} from 'components/HttpExecutor/utilities';

import HttpExecutorActions from 'components/HttpExecutor/store/HttpExecutorActions';
import { Map } from 'immutable';
import { REQUEST_HISTORY } from 'components/HttpExecutor/RequestHistoryTab';
import uuidV4 from 'uuid/v4';

// Limit number of log entries to be <= REQUEST_LOG_LIMIT.
const REQUEST_LOG_LIMIT = 200;

const defaultAction = {
  action: '',
  payload: {},
};

const defaultInitialState = {
  method: 'GET',
  path: '',
  body: '',
  headers: {
    pairs: [
      {
        key: '',
        value: '',
        uniqueId: uuidV4(),
      },
    ],
  },
  response: null,
  statusCode: 0,
  loading: false,
  activeTab: 0,
  requestLog: Map({}),
  saveCalls: true,
  selectedRequest: null,
};

const setResponse = (state, action) => {
  const { saveCalls } = state;
  const { response, statusCode } = action.payload;

  const newState = {
    ...state,
    response,
    statusCode,
    loading: false,
  };

  if (saveCalls) {
    return addRequestLog(newState);
  } else {
    return newState;
  }
};

const addRequestLog = (state) => {
  const { method, path, body, headers, response, statusCode, requestLog } = state;

  const newCall = {
    method,
    path,
    body,
    headers,
    response,
    statusCode,
    timestamp: new Date().toLocaleString(),
    id: uuidV4(),
  };

  // Update the component view in real-time, since we cannot listen to local storage's change
  // Since the new request call is the latest out of all the request histories, insert at 0th index
  const timestamp = new Date(newCall.timestamp);
  const dateID = getDateID(timestamp);
  const requestsGroup = getRequestsByDate(requestLog, dateID);
  let newRequestLog = requestLog.set(dateID, requestsGroup.insert(0, newCall));

  // Saving request histories to the localStorage
  let localStorageHistories = newRequestLog
    .valueSeq()
    .toJS()
    .flat();

  // Limit log entries to be <= REQUEST_LOG_LIMIT.
  // If the log entries count exceeds REQUEST_LOG_LIMIT, the oldest log should be removed.
  const logCount = localStorageHistories.length;
  if (logCount >= REQUEST_LOG_LIMIT) {
    const sortedDateIDs = requestLog
      .keySeq()
      .toArray()
      .sort((a, b) => compareByTimestamp(a, b));

    // Find the oldest date
    const oldestDateID = sortedDateIDs[sortedDateIDs.length - 1];

    // Find the oldest request
    const oldestRequests = getRequestsByDate(newRequestLog, oldestDateID);
    const oldestRequestID = oldestRequests.get(-1).timestamp;

    // Delete the oldest request
    state.requestLog = newRequestLog;
    newRequestLog = deleteRequestLog(state, oldestRequestID).requestLog;
    localStorageHistories = newRequestLog
      .valueSeq()
      .toJS()
      .flat();
  }

  localStorage.setItem(REQUEST_HISTORY, JSON.stringify(localStorageHistories));
  return {
    ...state,
    selectedRequest: newCall,
    requestLog: newRequestLog,
  };
};

const deleteRequestLog = (state, request) => {
  if (!request) {
    return;
  }
  const { requestLog } = state;

  // Delete the request log from local state
  const dateID = getDateID(new Date(request.timestamp));
  const requestsGroup = getRequestsByDate(requestLog, dateID);
  const requestToDelete = requestsGroup.findIndex((req) => req.id === request.id);
  const newRequestLog = requestLog.set(dateID, requestsGroup.delete(requestToDelete));

  // Delete the specified request log from local storage
  const storedLogs = newRequestLog
    .valueSeq()
    .toJS()
    .flat();
  localStorage.setItem(REQUEST_HISTORY, JSON.stringify(storedLogs));

  return {
    ...state,
    requestLog: newRequestLog,
  };
};

const clearAllRequestLog = (state) => {
  // Delete every request log from local state
  const newRequestLog = Map({});

  // Delete every request log from local storage
  localStorage.removeItem(REQUEST_HISTORY);

  return {
    ...state,
    requestLog: newRequestLog,
  };
};

const http = (state = defaultInitialState, action = defaultAction) => {
  switch (action.type) {
    case HttpExecutorActions.setMethod:
      return {
        ...state,
        method: action.payload.method,
        activeTab: ['GET', 'DELETE'].indexOf(action.payload.method) !== -1 ? 0 : 1,
      };
    case HttpExecutorActions.setPath:
      return {
        ...state,
        path: action.payload.path,
      };
    case HttpExecutorActions.enableLoading:
      return {
        ...state,
        loading: true,
      };
    case HttpExecutorActions.setResponse:
      return setResponse(state, action);
    case HttpExecutorActions.setBody:
      return {
        ...state,
        body: action.payload.body,
      };
    case HttpExecutorActions.setRequestTab:
      return {
        ...state,
        activeTab: action.payload.activeTab,
      };
    case HttpExecutorActions.setHeaders:
      return {
        ...state,
        headers: action.payload.headers,
      };
    case HttpExecutorActions.reset:
      return defaultInitialState;
    case HttpExecutorActions.setRequestLog:
      return {
        ...state,
        requestLog: action.payload.requestLog,
      };
    case HttpExecutorActions.setRequestHistoryView:
      return {
        ...state,
        method: action.payload.method,
        activeTab: ['GET', 'DELETE'].indexOf(action.payload.method) !== -1 ? 0 : 1,
        path: action.payload.path,
        response: action.payload.response,
        statusCode: action.payload.statusCode,
        body: action.payload.body,
        headers: action.payload.headers,
        selectedRequest: action.payload,
      };
    case HttpExecutorActions.toggleSaveCalls:
      return {
        ...state,
        saveCalls: !state.saveCalls,
      };
    case HttpExecutorActions.deleteRequestLog:
      return deleteRequestLog(state, action.payload.request);
    case HttpExecutorActions.clearAllRequestLog:
      return clearAllRequestLog(state);
    default:
      return state;
  }
};

const HttpExecutorStore = createStore(
  combineReducers({
    http,
  }),
  {
    http: defaultInitialState,
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default HttpExecutorStore;
