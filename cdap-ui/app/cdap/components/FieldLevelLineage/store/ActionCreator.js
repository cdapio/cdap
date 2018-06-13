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
import Store, {Actions} from 'components/FieldLevelLineage/store/Store';
import debounce from 'lodash/debounce';

export function getFields(datasetId, prefix) {
  let namespace = getCurrentNamespace();

  let params = {
    namespace,
    entityId: datasetId,
    start: 'now-7d',
    end: 'now'
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
  let namespace = getCurrentNamespace();
  let datasetId = Store.getState().lineage.datasetId;

  let params = {
    namespace,
    entityId: datasetId,
    fieldName,
    direction: 'backward',
    start: 'now-7d',
    end: 'now'
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
