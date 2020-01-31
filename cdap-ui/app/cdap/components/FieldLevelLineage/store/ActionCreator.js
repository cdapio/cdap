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

import { MyMetadataApi } from 'api/metadata';
import { getCurrentNamespace } from 'services/NamespaceStore';
import Store, { Actions, TIME_OPTIONS } from 'components/FieldLevelLineage/store/Store';
import debounce from 'lodash/debounce';
import { parseQueryString } from 'services/helpers';
import { Theme } from 'services/ThemeHelper';

export const TIME_OPTIONS_MAP = {
  [TIME_OPTIONS[1]]: {
    start: 'now-7d',
    end: 'now',
  },
  [TIME_OPTIONS[2]]: {
    start: 'now-14d',
    end: 'now',
  },
  [TIME_OPTIONS[3]]: {
    start: 'now-30d',
    end: 'now',
  },
  [TIME_OPTIONS[4]]: {
    start: 'now-180d',
    end: 'now',
  },
  [TIME_OPTIONS[5]]: {
    start: 'now-365d',
    end: 'now',
  },
};

function getTimeRange() {
  const store = Store.getState();
  const selection = store.lineage.timeSelection;

  if (selection === TIME_OPTIONS[0]) {
    return {
      start: store.customTime.start || 'now-7d',
      end: store.customTime.end || 'now',
    };
  }

  return TIME_OPTIONS_MAP[selection];
}

function getFirstFieldLineage(fields) {
  let chosenField;
  for (let i = 0; i < fields.length; i++) {
    if (fields[i].lineage) {
      chosenField = fields[i].name;
      break;
    }
  }

  if (chosenField) {
    getLineageSummary(chosenField);
  } else {
    replaceHistory();
  }
}

export function init(datasetId) {
  const queryParams = parseQueryString() || {};

  let timeObj = TIME_OPTIONS_MAP[TIME_OPTIONS[1]];

  if (queryParams.time && TIME_OPTIONS.indexOf(queryParams.time) !== -1) {
    Store.dispatch({
      type: Actions.setTimeSelection,
      payload: {
        timeSelection: queryParams.time,
      },
    });

    timeObj = TIME_OPTIONS_MAP[queryParams.time];
  }

  if (queryParams.time === TIME_OPTIONS[0]) {
    timeObj = {
      start: parseInt(queryParams.start, 10),
      end: parseInt(queryParams.end, 10),
    };

    Store.dispatch({
      type: Actions.setCustomTime,
      payload: {
        start: timeObj.start,
        end: timeObj.end,
      },
    });
  }

  const params = {
    namespace: getCurrentNamespace(),
    entityId: datasetId,
    start: timeObj.start,
    end: timeObj.end,
    includeCurrent: true,
  };

  MyMetadataApi.getFields(params).subscribe((fields) => {
    Store.dispatch({
      type: Actions.setFields,
      payload: {
        datasetId,
        fields,
      },
    });

    if (queryParams.field) {
      getLineageSummary(queryParams.field);
      return;
    }

    // If there is no query parameter for selected field, choose first field that have lineage
    getFirstFieldLineage(fields);
  });
}

export function getFields(datasetId, prefix, start = 'now-7d', end = 'now') {
  Store.dispatch({
    type: Actions.closeSummary,
  });

  const namespace = getCurrentNamespace();

  let params = {
    namespace,
    entityId: datasetId,
    start,
    end,
    includeCurrent: true,
  };

  if (prefix && prefix.length > 0) {
    params.prefix = prefix;
  }

  MyMetadataApi.getFields(params).subscribe((res) => {
    Store.dispatch({
      type: Actions.setFields,
      payload: {
        datasetId,
        fields: res,
      },
    });

    getFirstFieldLineage(res);
  });
}

export function getLineageSummary(fieldName) {
  const namespace = getCurrentNamespace();
  const datasetId = Store.getState().lineage.datasetId;
  const { start, end } = getTimeRange();

  const params = {
    namespace,
    entityId: datasetId,
    fieldName,
    direction: 'both',
    start,
    end,
  };

  MyMetadataApi.getFieldLineage(params).subscribe((res) => {
    Store.dispatch({
      type: Actions.setLineageSummary,
      payload: {
        incoming: res.incoming,
        outgoing: res.outgoing,
        activeField: fieldName,
      },
    });

    replaceHistory();
  });
}

const debouncedGetFields = debounce(getFields, 500);

export function search(e) {
  const datasetId = Store.getState().lineage.datasetId;
  const searchText = e.target.value;

  Store.dispatch({
    type: Actions.setSearch,
    payload: {
      search: searchText,
    },
  });

  debouncedGetFields(datasetId, searchText);
}

export function getOperations(direction) {
  Store.dispatch({
    type: Actions.operationsLoading,
    payload: {
      direction,
    },
  });

  const state = Store.getState().lineage;
  const entityId = state.datasetId;
  const fieldName = state.activeField;
  const namespace = getCurrentNamespace();
  const { start, end } = getTimeRange();

  const params = {
    namespace,
    entityId,
    fieldName,
    start,
    end,
    direction,
  };

  MyMetadataApi.getFieldOperations(params).subscribe((res) => {
    Store.dispatch({
      type: Actions.setOperations,
      payload: {
        operations: res[direction],
        direction,
      },
    });
  });
}

export function setCustomTimeRange({ start, end }) {
  Store.dispatch({
    type: Actions.setCustomTime,
    payload: {
      start,
      end,
    },
  });

  const state = Store.getState().lineage;

  getFields(state.datasetId, state.search, start, end);

  replaceHistory();
}

export function setTimeRange(option) {
  if (TIME_OPTIONS.indexOf(option) === -1) {
    return;
  }

  Store.dispatch({
    type: Actions.setTimeSelection,
    payload: {
      timeSelection: option,
    },
  });

  if (option === TIME_OPTIONS[0]) {
    return;
  }

  const { start, end } = TIME_OPTIONS_MAP[option];
  const state = Store.getState().lineage;

  getFields(state.datasetId, state.search, start, end);

  replaceHistory();
}

export function getTimeQueryParams() {
  const state = Store.getState();

  let url = `time=${state.lineage.timeSelection}`;

  if (state.lineage.timeSelection === TIME_OPTIONS[0]) {
    url += `&start=${state.customTime.start}&end=${state.customTime.end}`;
  }

  return url;
}

export function replaceHistory() {
  const url = constructQueryParamsURL();
  const currentLocation = location.pathname + location.search;

  if (url === currentLocation) {
    return;
  }

  const stateObj = {
    title: Theme.productName,
    url,
  };

  // This timeout is to make sure rendering by store update is finished before changing the state.
  // Otherwise, some fonts will not render because it is referencing wrong path.
  setTimeout(() => {
    history.replaceState(stateObj, stateObj.title, stateObj.url);
  });
}

function constructQueryParamsURL() {
  const state = Store.getState();

  let url = location.pathname;

  url += `?${getTimeQueryParams()}`;

  if (state.lineage.activeField) {
    url += `&field=${state.lineage.activeField}`;
  }

  return url;
}
