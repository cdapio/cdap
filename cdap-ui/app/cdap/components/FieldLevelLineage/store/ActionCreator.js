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

import {MyMetadataApi} from 'api/metadata';
import {getCurrentNamespace} from 'services/NamespaceStore';
import Store, {Actions, TIME_OPTIONS} from 'components/FieldLevelLineage/store/Store';
import debounce from 'lodash/debounce';

const TIME_OPTIONS_MAP = {
  [TIME_OPTIONS[0]]: {
    start: 'now-7d',
    end: 'now'
  },
  [TIME_OPTIONS[1]]: {
    start: 'now-14d',
    end: 'now'
  },
  [TIME_OPTIONS[2]]: {
    start: 'now-30d',
    end: 'now'
  },
  [TIME_OPTIONS[3]]: {
    start: 'now-180d',
    end: 'now'
  },
  [TIME_OPTIONS[4]]: {
    start: 'now-365d',
    end: 'now'
  }
};

function getTimeRange() {
  const selection = Store.getState().lineage.timeSelection;

  return TIME_OPTIONS_MAP[selection];
}

export function getFields(datasetId, prefix, start = 'now-7d', end = 'now') {
  const namespace = getCurrentNamespace();

  let params = {
    namespace,
    entityId: datasetId,
    start,
    end
  };

  if (prefix && prefix.length > 0) {
    params.prefix = prefix;
  }

  MyMetadataApi.getFields(params)
    .subscribe((res) => {
      Store.dispatch({
        type: Actions.setFields,
        payload: {
          datasetId,
          fields: res
        }
      });
    });
}

export function getLineageSummary(fieldName) {
  const namespace = getCurrentNamespace();
  const datasetId = Store.getState().lineage.datasetId;
  const {start, end} = getTimeRange();

  const params = {
    namespace,
    entityId: datasetId,
    fieldName,
    direction: 'backward',
    start,
    end
  };

  MyMetadataApi.getFieldLineage(params)
    .subscribe((res) => {
      Store.dispatch({
        type: Actions.setBackwardLineage,
        payload: {
          backward: res.backward,
          activeField: fieldName
        }
      });
    });
}

const debouncedGetFields = debounce(getFields, 500);

export function search(e) {
  const datasetId = Store.getState().lineage.datasetId;
  const searchText = e.target.value;

  Store.dispatch({
    type: Actions.setSearch,
    payload: {
      search: searchText
    }
  });

  debouncedGetFields(datasetId, searchText);
}

export function getOperations() {
  Store.dispatch({
    type: Actions.operationsLoading
  });

  const state = Store.getState().lineage;
  const entityId = state.datasetId;
  const fieldName = state.activeField;
  const namespace = getCurrentNamespace();
  const {start, end} = getTimeRange();

  const params = {
    namespace,
    entityId,
    fieldName,
    start,
    end,
    direction: 'backward'
  };

  MyMetadataApi.getFieldOperations(params)
    .subscribe((res) => {
      Store.dispatch({
        type: Actions.setBackwardOperations,
        payload: {
          backwardOperations: res.backward
        }
      });
    });
}

export function setTimeRange(option) {
  Store.dispatch({
    type: Actions.setTimeSelection,
    payload: {
      timeSelection: option
    }
  });

  const {start, end} = TIME_OPTIONS_MAP[option];
  const state = Store.getState().lineage;

  getFields(state.datasetId, state.search, start, end);
}
