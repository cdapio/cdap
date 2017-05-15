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
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';

const defaultAction = {
  action : '',
  payload : {}
};

const defaultInitialState = {
  initialized: false,
  workspaceId: '',
  workspaceUri: '',
  data: [],
  headers: [],
  selectedHeaders: [],
  highlightColumns: {
    directive: null,
    columns: []
  },
  directives: [],
  higherVersion: null,
  loading: false,
  singleWorkspaceMode: false,
  workspaceInfo: null
};

const errorInitialState = {
  showError: null,
  cliError: null
};

const columnsInformationInitialState = {
  columns: {}
};

const workspacesInitialState = {
  list: []
};

const dataprep = (state = defaultInitialState, action = defaultAction) => {
  let stateCopy;

  switch (action.type) {
    case DataPrepActions.setData:
      stateCopy = Object.assign({}, state, {
        data: action.payload.data,
        headers: action.payload.headers,
        loading: false
      });
      break;
    case DataPrepActions.setDirectives:
      stateCopy = Object.assign({}, state, {
        data: action.payload.data,
        headers: action.payload.headers,
        directives: action.payload.directives,
        loading: false,
        // after any directive, remove selected header(s) if they're no longer in
        // the list of headers
        selectedHeaders: state.selectedHeaders.filter((head) => {
          return action.payload.headers.indexOf(head) !== -1;
        })
      });
      break;
    case DataPrepActions.setWorkspace:
      stateCopy = Object.assign({}, state, {
        workspaceId: action.payload.workspaceId,
        workspaceUri: action.payload.workspaceUri,
        headers: action.payload.headers || [],
        directives: action.payload.directives || [],
        data: action.payload.data || [],
        initialized: true,
        loading: false,
        selectedHeaders: [],
        workspaceInfo: action.payload.workspaceInfo
      });
      break;
    case DataPrepActions.setSelectedHeaders:
      stateCopy = Object.assign({}, state, {
        selectedHeaders: action.payload.selectedHeaders
      });
      break;
    case DataPrepActions.setInitialized:
      stateCopy = Object.assign({}, state, { initialized: true });
      break;
    case DataPrepActions.setHigherVersion:
      stateCopy = Object.assign({}, state, {
        higherVersion: action.payload.higherVersion
      });
      break;
    case DataPrepActions.setWorkspaceMode:
      stateCopy = Object.assign({}, state, {
        singleWorkspaceMode: action.payload.singleWorkspaceMode
      });
      break;
    case DataPrepActions.setHighlightColumns:
      stateCopy = Object.assign({}, state, {
        highlightColumns: action.payload.highlightColumns
      });
      break;
    case DataPrepActions.enableLoading:
      stateCopy = Object.assign({}, state, {
        loading: true
      });
      break;
    case DataPrepActions.disableLoading:
      stateCopy = Object.assign({}, state, {
        loading: false
      });
      break;
    case DataPrepActions.reset:
      return defaultInitialState;
    default:
      return state;
  }

  return Object.assign({}, state, stateCopy);
};

const error = (state = errorInitialState, action = defaultAction) => {
  let stateCopy;

  switch (action.type) {
    case DataPrepActions.setError:
      stateCopy = Object.assign({}, state, {
        showError: action.payload.message,
        cliError: null
      });
      break;
    case DataPrepActions.setCLIError:
      stateCopy = Object.assign({}, state, {
        showError: null,
        cliError: action.payload.message
      });
      break;
    case DataPrepActions.setWorkspace:
    case DataPrepActions.setDirectives:
      stateCopy = Object.assign({}, state, {
        showError: null,
        cliError: null
      });
      break;
    case DataPrepActions.dismissError:
      stateCopy = Object.assign({}, state, {
        showError: null
      });
      break;
    case DataPrepActions.reset:
      return errorInitialState;
    default:
      return state;
  }

  return Object.assign({}, state, stateCopy);
};

const columnsInformation = (state = columnsInformationInitialState, action = defaultAction) => {
  let stateCopy;

  switch (action.type) {
    case DataPrepActions.setColumnsInformation:
      stateCopy = Object.assign({}, state, {
        columns: action.payload.columns
      });
      break;
    case DataPrepActions.reset:
      return columnsInformationInitialState;
    default:
      return state;
  }

  return Object.assign({}, state, stateCopy);
};

const workspaces = (state = workspacesInitialState, action = defaultAction) => {
  let stateCopy;

  switch (action.type) {
    case DataPrepActions.setWorkspaceList:
      stateCopy = Object.assign({}, state, {
        list: action.payload.list
      });
      break;
    case DataPrepActions.reset:
      return workspacesInitialState;
    default:
      return state;
  }

  return Object.assign({}, state, stateCopy);
};

const DataPrepStore = createStore(
  combineReducers({
    dataprep,
    error,
    columnsInformation,
    workspaces
  }),
  {
    dataprep: defaultInitialState,
    error: errorInitialState,
    workspaces: workspacesInitialState
  },
  window.__REDUX_DEVTOOLS_EXTENSION__ && window.__REDUX_DEVTOOLS_EXTENSION__()
);

export default DataPrepStore;
