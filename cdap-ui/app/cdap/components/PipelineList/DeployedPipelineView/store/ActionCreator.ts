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

import { getCurrentNamespace } from 'services/NamespaceStore';
import { GLOBALS } from 'services/global-constants';
import { MyPipelineApi } from 'api/pipeline';
import Store, { Actions, SORT_ORDER } from 'components/PipelineList/DeployedPipelineView/store';
import { IPipeline } from 'components/PipelineList/DeployedPipelineView/types';
import orderBy from 'lodash/orderBy';

export function fetchPipelineList() {
  const namespace = getCurrentNamespace();

  const params = {
    namespace,
    artifactName: GLOBALS.etlPipelineTypes.join(','),
  };

  // TODO do not get pipelines with the old method
  MyPipelineApi.list(params).subscribe((res: IPipeline[]) => {
    const pipelines = orderBy(res, [(pipeline) => pipeline.name.toLowerCase()], ['asc']);

    Store.dispatch({
      type: Actions.setPipeline,
      payload: {
        pipelines,
      },
    });
  });
}

export function deletePipeline(pipeline: IPipeline) {
  const namespace = getCurrentNamespace();

  const params = {
    namespace,
    appId: pipeline.name,
  };

  MyPipelineApi.delete(params).subscribe(fetchPipelineList, (err) => {
    Store.dispatch({
      type: Actions.setDeleteError,
      payload: {
        deleteError: err,
      },
    });
  });
}

export function reset() {
  Store.dispatch({
    type: Actions.reset,
  });
}

export function setSort(columnName: string) {
  const state = Store.getState().deployed;
  const currentColumn = state.sortColumn;
  const currentSortOrder = state.sortOrder;

  let sortOrder = SORT_ORDER.asc;
  if (currentColumn === columnName && currentSortOrder === SORT_ORDER.asc) {
    sortOrder = SORT_ORDER.desc;
  }

  Store.dispatch({
    type: Actions.setSort,
    payload: {
      sortColumn: columnName,
      sortOrder,
    },
  });
}
