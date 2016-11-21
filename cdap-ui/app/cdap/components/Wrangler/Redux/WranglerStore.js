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
import WranglerActions from 'components/Wrangler/Redux/WranglerActions';
import shortid from 'shortid';
import cloneDeep from 'lodash/cloneDeep';
import {createBucket} from 'components/Wrangler/data-buckets';
import {inferColumn} from 'components/Wrangler/type-inference';
import {
  dropColumn,
  renameColumn,
  splitColumn,
  mergeColumn,
  uppercaseColumn,
  lowercaseColumn,
  titlecaseColumn,
  substringColumn
} from 'components/Wrangler/column-transforms';

const defaultAction = {
  type: '',
  payload: {}
};

const defaultInitialState = {
  wrangler: {
    headersList: [],
    originalData: [],
    data: [],
    errors: {},
    history: [],
    historyLocation: 0,
    histogram: {},
    columnTypes: {},
    filter: null,
    sort: null,
    sortAscending: true
  }
};

const wrangler = (state = defaultInitialState, action = defaultAction) => {
  let stateCopy;
  let data;
  switch (action.type) {
    case WranglerActions.setData:
      data = _setData(action.payload);
      return Object.assign({}, state, data);
    case WranglerActions.dropColumn:
      stateCopy = Object.assign({}, state);
      data = _dropColumn(stateCopy, action.payload);
      stateCopy = Object.assign({}, stateCopy, data);

      break;
    case WranglerActions.splitColumn:
      stateCopy = Object.assign({}, state);
      data = _splitColumn(stateCopy, action.payload);
      stateCopy = Object.assign({}, stateCopy, data);

      break;
    case WranglerActions.mergeColumn:
      stateCopy = Object.assign({}, state);
      data = _mergeColumn(stateCopy, action.payload);
      stateCopy = Object.assign({}, stateCopy, data);

      break;
    case WranglerActions.renameColumn:
      stateCopy = Object.assign({}, state);
      data = _renameColumn(stateCopy, action.payload);
      stateCopy = Object.assign({}, stateCopy, data);

      break;
    case WranglerActions.upperCaseColumn:
      stateCopy = Object.assign({}, state);
      data = _transformCaseColumn(stateCopy, 'UPPERCASE', action.payload);
      stateCopy = Object.assign({}, stateCopy, data);

      break;
    case WranglerActions.lowerCaseColumn:
      stateCopy = Object.assign({}, state);
      data = _transformCaseColumn(stateCopy, 'LOWERCASE', action.payload);
      stateCopy = Object.assign({}, stateCopy, data);

      break;
    case WranglerActions.titleCaseColumn:
      stateCopy = Object.assign({}, state);
      data = _transformCaseColumn(stateCopy, 'TITLECASE', action.payload);
      stateCopy = Object.assign({}, stateCopy, data);

      break;
    case WranglerActions.subStringColumn:
      stateCopy = Object.assign({}, state);
      data = _substringColumn(stateCopy, action.payload);
      stateCopy = Object.assign({}, stateCopy, data);

      break;
    case WranglerActions.sortColumn:
      return Object.assign({}, state, {
        sort: action.payload.activeColumn,
        sortAscending: state.sort && state.sort === action.payload.activeColumn ? !state.sortAscending : true
      });
    case WranglerActions.setFilter:
      return Object.assign({}, state, {
        filter: action.payload.filter
      });
    case WranglerActions.deleteHistory:
      return _deleteHistory(state, action.payload);
    case WranglerActions.undo:
      return _deleteHistory(state, { index: state.historyLocation - 1 });
    case WranglerActions.redo:
      return _forwardHistory(state);
    case WranglerActions.reset:
      return defaultInitialState;

    default:
      return Object.assign({}, state);
  }

  return Object.assign({}, state, stateCopy, addHistory(stateCopy, action.type, action.payload));
};

function _setData(payload) {
  const headersList = Object.keys(payload.data[0]);
  const data = payload.data;
  const originalData = cloneDeep(data);
  const errors = {};
  let columnTypes = {};
  let histogram = {};

  headersList.forEach((column) => {
    let columnType = inferColumn(data, column);
    columnTypes[column] = columnType;

    histogram[column] = createBucket(data, column, columnType);
    errors[column] = detectNullInColumm(data, column);
  });

  return {
    data,
    originalData,
    headersList,
    errors,
    columnTypes,
    histogram
  };
}

function _deleteHistory(state, payload) {
  if (state.historyLocation === 0) { return state; }

  let newHistory = state.history.slice(0, payload.index);
  let historyLocation = payload.index;

  let history = cloneDeep(state.history);

  let newPayload = {
    data: cloneDeep(state.originalData)
  };

  let stateCopy = Object.assign({}, defaultInitialState.wrangler, _setData(newPayload));

  newHistory.forEach((history) => {
    stateCopy = wrangler(stateCopy, {
      type: history.action,
      payload: history.payload
    });
  });

  return Object.assign({}, stateCopy, {
    history,
    historyLocation
  });
}

function _forwardHistory(state) {
  if (state.history.length === state.historyLocation) {
    return state;
  }

  let stateCopy = Object.assign({}, state);

  let historyLocation = state.historyLocation + 1;
  let history = cloneDeep(state.history);
  let forwardHistory = history[state.historyLocation];

  stateCopy = wrangler(stateCopy, {
    type: forwardHistory.action,
    payload: forwardHistory.payload
  });

  return Object.assign({}, stateCopy, {
    history,
    historyLocation
  });
}

function addHistory(state, type, payload) {
  let historyObj = {
    id: shortid.generate(),
    action: type,
    payload
  };

  let history = state.history;
  let limit = state.historyLocation;

  history = history.slice(0, limit);
  history.push(historyObj);

  let historyLocation = history.length;

  return {
    history,
    historyLocation
  };
}

function _dropColumn(state, payload) {
  const columnToDrop = payload.activeColumn;

  let data = dropColumn(state.data, columnToDrop);
  let metadata = removeColumnMetadata(state, [columnToDrop]);

  return Object.assign({}, metadata, { data });
}

function _splitColumn(state, payload) {
  const columnToSplit = payload.activeColumn;
  const delimiter = payload.delimiter;
  const firstSplit = payload.firstSplit;
  const secondSplit = payload.secondSplit;

  let data = splitColumn(state.data, delimiter, columnToSplit, firstSplit, secondSplit);

  const index = state.headersList.indexOf(columnToSplit);
  let metadata = addColumnMetadata(state, [firstSplit, secondSplit], index+1, data);

  return Object.assign({}, metadata, { data });
}

function _mergeColumn(state, payload) {
  const mergeWith = payload.mergeWith;
  const columnToMerge = payload.activeColumn;
  const joinBy = payload.joinBy;
  const columnName = payload.mergedColumnName;

  let data = mergeColumn(state.data, joinBy, columnToMerge, mergeWith, columnName);

  const index = state.headersList.indexOf(columnToMerge);
  let metadata = addColumnMetadata(state, [columnName], index+1, data);

  return Object.assign({}, metadata, { data });
}

function _renameColumn(state, payload) {
  const originalName = payload.activeColumn;
  const newName = payload.newName;

  let data = renameColumn(state.data, originalName, newName);
  let metadata = renameColumnMetadata(state, originalName, newName);

  return Object.assign({}, metadata, { data });
}

function _transformCaseColumn(state, type, payload) {
  const columnToTransform = payload.activeColumn;

  let data;
  switch (type) {
    case 'UPPERCASE':
      data = uppercaseColumn(state.data, columnToTransform);
      break;
    case 'LOWERCASE':
      data = lowercaseColumn(state.data, columnToTransform);
      break;
    case 'TITLECASE':
      data = titlecaseColumn(state.data, columnToTransform);
      break;
  }

  return Object.assign({}, { data });
}

function _substringColumn(state, payload) {
  const columnToSub = payload.activeColumn;
  const beginIndex = payload.beginIndex;
  const endIndex = payload.endIndex;
  const substringColumnName = payload.columnName;

  let data = substringColumn(state.data, columnToSub, beginIndex, endIndex, substringColumnName);

  const index = state.headersList.indexOf(columnToSub);
  let metadata = addColumnMetadata(state, [substringColumnName], index+1, data);

  return Object.assign({}, metadata, { data });
}

function detectNullInColumm(data, column) {
  let errorObject = {count: 0};
  data.forEach((row, index) => {
    if (row[column] === null || !row[column]) {
      errorObject[index] = true;
      errorObject.count++;
    }
  });

  return errorObject;
}

function renameColumnMetadata(state, oldName, newName) {
  let headersList = state.headersList;
  headersList[headersList.indexOf(oldName)] = newName;

  let columnTypes = state.columnTypes;
  columnTypes[newName] = columnTypes[oldName];
  delete columnTypes[oldName];

  let histogram = state.histogram;
  histogram[newName] = histogram[oldName];
  delete histogram[oldName];

  let errors = state.errors;
  errors[newName] = errors[oldName];
  delete errors[oldName];

  return {
    headersList,
    columnTypes,
    histogram,
    errors
  };
}

function addColumnMetadata(state, columns, index, data) {
  let headersList = state.headersList;
  let columnTypes = state.columnTypes;
  let histogram = state.histogram;
  let errors = state.errors;

  columns.forEach((column, i) => {
    headersList.splice(index+i, 0, column);

    let columnType = inferColumn(data, column);
    columnTypes[column] = columnType;
    histogram[column] = createBucket(data, column, columnType);
    errors[column] = detectNullInColumm(data, column);
  });

  return {
    headersList,
    columnTypes,
    histogram,
    errors
  };
}

function removeColumnMetadata(state, columns) {
  let headersList = state.headersList;
  let columnTypes = state.columnTypes;
  let histogram = state.histogram;
  let errors = state.errors;

  columns.forEach((column) => {
    headersList.splice(headersList.indexOf(column), 1);
    delete columnTypes[column];
    delete histogram[column];
    delete errors[column];
  });

  return {
    headersList,
    columnTypes,
    histogram,
    errors
  };
}

const WranglerStoreWrapper = () => {
  return createStore(
    combineReducers({
      wrangler
    }),
    defaultInitialState
  );
};

const WranglerStore = WranglerStoreWrapper();
export default WranglerStore;
