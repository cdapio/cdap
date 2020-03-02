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
import { MyPipelineApi } from 'api/pipeline';
import Store, {
  Actions,
  SORT_ORDER,
  IStore,
} from 'components/PipelineList/DeployedPipelineView/store';
import { IPipeline } from 'components/PipelineList/DeployedPipelineView/types';
import { orderBy } from 'natural-orderby';
import { objectQuery } from 'services/helpers';
import { PROGRAM_STATUSES, GLOBALS } from 'services/global-constants';
import debounce from 'lodash/debounce';
const debounceFilteredPipelineRunsFetch = debounce(getRunsForFilteredPipelines, 500);

function getOrderColumnFunction(sortColumn, sortOrder) {
  switch (sortColumn) {
    case 'name':
      return (pipeline) => pipeline.name.toLowerCase();
    case 'type':
      return (pipeline) => pipeline.artifact.name;
    case 'status':
      return (pipeline) => {
        return objectQuery(pipeline, 'runs', 0, 'status') || PROGRAM_STATUSES.DEPLOYED;
      };
    case 'lastStartTime':
      return (pipeline) => {
        const lastStarting = objectQuery(pipeline, 'runs', 0, 'starting');

        if (!lastStarting) {
          return sortOrder === SORT_ORDER.asc ? Infinity : -1;
        }
        return lastStarting;
      };
    case 'runs':
      return (pipeline) => {
        return pipeline.totalRuns || 0;
      };
  }
}

export function deletePipeline(pipeline: IPipeline, refetch: () => void) {
  const namespace = getCurrentNamespace();

  const params = {
    namespace,
    appId: pipeline.name,
  };

  MyPipelineApi.delete(params).subscribe(
    () => {
      refetch();
      reset();
    },
    (err) => {
      Store.dispatch({
        type: Actions.setDeleteError,
        payload: {
          deleteError: err,
        },
      });
    }
  );
}

export function reset() {
  Store.dispatch({
    type: Actions.reset,
  });
}

function getRunsForFilteredPipelines() {
  const { filteredPipelines, pipelines } = Store.getState().deployed;
  if (!filteredPipelines || !pipelines) {
    return;
  }
  const pipelinesWithoutRuns = filteredPipelines.filter(
    (pipeline) => pipeline.runs === null || pipeline.totalRuns === null
  );
  if (!pipelinesWithoutRuns.length) {
    return;
  }
  const getProgram = (pipelineType) => {
    const programId = GLOBALS.programId[pipelineType];
    const programType = GLOBALS.programTypeName[pipelineType];
    return {
      programId,
      programType,
    };
  };
  const postBody = pipelinesWithoutRuns.map((pipeline) => ({
    appId: pipeline.name,
    ...getProgram(pipeline.artifact.name),
  }));
  const nextRuntimePostBody = pipelinesWithoutRuns
    .filter((pipeline) => pipeline.artifact.name === GLOBALS.etlDataPipeline)
    .map((pipeline) => ({
      appId: pipeline.name,
      ...getProgram(pipeline.artifact.name),
    }));
  const namespace = getCurrentNamespace();
  MyPipelineApi.getBatchRuns({ namespace }, postBody)
    .combineLatest(
      MyPipelineApi.getRunsCount({ namespace }, postBody),
      MyPipelineApi.batchGetNextRunTime({ namespace }, nextRuntimePostBody)
    )
    .subscribe(([runs, runsCount, nextRuntime]) => {
      const runsMap = Object.assign({}, ...runs.map((app) => ({ [app.appId]: app.runs })));
      const nextRuntimeMap = Object.assign(
        {},
        ...nextRuntime.map((app) => ({ [app.appId]: app.schedules }))
      );
      const runsCountMap = Object.assign(
        {},
        ...runsCount.map((app) => ({ [app.appId]: app.runCount }))
      );
      const getNextrunTime = (nrMap, pipeline) => {
        if (pipeline.artifact.name === GLOBALS.etlDataStreams) {
          return [];
        }
        return nrMap[pipeline.name] || pipeline.nextRuntime;
      };
      const filteredPipelinesWithRuns = filteredPipelines.map((pipeline) => {
        return {
          ...pipeline,
          runs: runsMap[pipeline.name] || pipeline.runs,
          totalRuns: runsCountMap[pipeline.name] || pipeline.totalRuns,
          nextRuntime: getNextrunTime(nextRuntimeMap, pipeline),
        };
      });
      const pipelinesWithRuns = pipelines.map((pipeline) => {
        return {
          ...pipeline,
          runs: runsMap[pipeline.name] || pipeline.runs,
          totalRuns: runsCountMap[pipeline.name] || pipeline.totalRuns,
          nextRuntime: getNextrunTime(nextRuntimeMap, pipeline),
        };
      });
      Store.dispatch({
        type: Actions.updateFilteredPipelines,
        payload: {
          filteredPipelines: filteredPipelinesWithRuns,
          pipelines: pipelinesWithRuns,
        },
      });
    });
}

function getFilteredPipelines({
  pipelines,
  search,
  sortOrder,
  sortColumn,
  currentPage,
  pageLimit,
}: IStore['deployed']): IPipeline[] {
  if (!pipelines) {
    return;
  }
  let filteredPipelines = pipelines;
  if (search.length > 0) {
    filteredPipelines = pipelines.filter((pipeline) => {
      const name = pipeline.name.toLowerCase();
      const searchFilter = search.toLowerCase();

      return name.indexOf(searchFilter) !== -1;
    });
  }
  filteredPipelines = orderBy(
    filteredPipelines,
    [getOrderColumnFunction(sortColumn, sortOrder)],
    [sortOrder]
  );

  const startIndex = (currentPage - 1) * pageLimit;
  const endIndex = startIndex + pageLimit;
  filteredPipelines = filteredPipelines.slice(startIndex, endIndex);
  return filteredPipelines;
}

export function setFilteredPipelines(pipelines = Store.getState().deployed.pipelines) {
  const filteredPipelines = getFilteredPipelines({ ...Store.getState().deployed, pipelines });

  Store.dispatch({
    type: Actions.setPipelines,
    payload: {
      pipelines,
      filteredPipelines,
    },
  });
  getRunsForFilteredPipelines();
}

export function setPage(currentPage) {
  const { currentPage: currentPageFromStore } = Store.getState().deployed;
  if (!currentPage) {
    currentPage = currentPageFromStore;
  }
  const filteredPipelines = getFilteredPipelines({ ...Store.getState().deployed, currentPage });
  Store.dispatch({
    type: Actions.setPage,
    payload: {
      currentPage,
      filteredPipelines,
    },
  });
  getRunsForFilteredPipelines();
}

export function setSearch(searchText: string) {
  const newState = {
    ...Store.getState().deployed,
    search: searchText,
  };
  if (!searchText) {
    newState.currentPage = 1;
  }
  const filteredPipelines = getFilteredPipelines(newState);
  Store.dispatch({
    type: Actions.setSearch,
    payload: {
      search: searchText,
      filteredPipelines,
    },
  });
  debounceFilteredPipelineRunsFetch();
}

export function setSort(columnName: string) {
  const state = Store.getState().deployed;
  const currentColumn = state.sortColumn;
  const currentSortOrder = state.sortOrder;

  let sortOrder = SORT_ORDER.asc;
  if (currentColumn === columnName && currentSortOrder === SORT_ORDER.asc) {
    sortOrder = SORT_ORDER.desc;
  }

  const filteredPipelines = getFilteredPipelines({
    ...Store.getState().deployed,
    sortColumn: columnName,
    sortOrder,
    currentPage: 1,
  });

  Store.dispatch({
    type: Actions.setSort,
    payload: {
      sortColumn: columnName,
      sortOrder,
      filteredPipelines,
    },
  });
  getRunsForFilteredPipelines();
}
