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
import { objectQuery } from 'services/helpers';
import { PROGRAM_STATUSES } from 'services/global-constants';
import {
  IPipeline,
  IPipelineStatus,
  IStatusMap,
  IRunsCountMap,
} from 'components/PipelineList/DeployedPipelineView/types';
import orderBy from 'lodash/orderBy';
import StatusMapper from 'services/StatusMapper';

const ProgramType = {
  [GLOBALS.etlDataPipeline]: 'Workflow',
  [GLOBALS.etlDataStreams]: 'Spark',
};

interface IPipelineParams {
  appId: string;
  programType: string;
  programId: string;
}

const DEFAULT_STATUS: IPipelineStatus = {
  status: PROGRAM_STATUSES.DEPLOYED,
  lastStarting: null,
};

export function fetchPipelineList() {
  const namespace = getCurrentNamespace();

  const params = {
    namespace,
    artifactName: GLOBALS.etlPipelineTypes.join(','),
  };

  MyPipelineApi.list(params).subscribe((res: IPipeline[]) => {
    const pipelines = orderBy(res, [(pipeline) => pipeline.name.toLowerCase()], ['asc']);

    Store.dispatch({
      type: Actions.setPipeline,
      payload: {
        pipelines,
      },
    });

    fetchRunsInfo();
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

function fetchRunsInfo() {
  const namespace = getCurrentNamespace();
  const pipelines: IPipelineParams[] = Store.getState().deployed.pipelines.map((pipeline) => {
    const programInfo = GLOBALS.programInfo[pipeline.artifact.name];

    return {
      appId: pipeline.name,
      programType: ProgramType[pipeline.artifact.name],
      programId: programInfo.programName,
    };
  });

  fetchRuns(namespace, pipelines);
  fetchRunsCount(namespace, pipelines);
}

/**
 *
 * @param namespace
 * @param pipelines array of pipeline objects with appId, programType, programId
 *
 * Will dispatch an event with the statusMap as the payload
 *
 * e.g:
 * {
 *    'Pipeline1': {
 *      status: 'RUNNING',
 *      lastStarting: 1542669738
 *    },
 *    'Pipeline2': { ... },
 *    ...
 * }
 */
function fetchRuns(namespace: string, pipelines: IPipelineParams[]) {
  MyPipelineApi.getBatchRuns({ namespace }, pipelines).subscribe((res) => {
    const statusMap: IStatusMap = {};

    res.forEach((pipeline) => {
      const latestRun = objectQuery(pipeline, 'runs', 0) || DEFAULT_STATUS;
      const displayStatus = StatusMapper.lookupDisplayStatus(latestRun.status);

      statusMap[pipeline.appId] = {
        status: latestRun.status,
        displayStatus,
        lastStarting: latestRun.starting,
      };
    });

    Store.dispatch({
      type: Actions.setStatusMap,
      payload: {
        statusMap,
      },
    });
  });
}

/**
 *
 * @param namespace
 * @param pipelines array of pipeline objects with appId, programType, programId
 *
 * Will dispatch an event with the runsCount as the payload
 *
 * e.g:
 * {
 *    'Pipeline1': 27,
 *    'Pipeline2': 35,
 *    ...
 * }
 */
function fetchRunsCount(namespace: string, pipelines: IPipelineParams[]) {
  MyPipelineApi.getRunsCount({ namespace }, pipelines).subscribe((res) => {
    const runsCountMap: IRunsCountMap = {};

    res.forEach((pipeline) => {
      runsCountMap[pipeline.appId] = pipeline.runCount || 0;
    });

    Store.dispatch({
      type: Actions.setRunsCountMap,
      payload: {
        runsCountMap,
      },
    });
  });
}

export function setSort(columnName: string) {
  const state = Store.getState().deployed;
  const currentColumn = state.sortColumn;
  const currentSortOrder = state.sortOrder;
  const statusMap = state.statusMap;
  const runsCountMap = state.runsCountMap;

  let sortOrder = SORT_ORDER.asc;
  if (currentColumn === columnName && currentSortOrder === SORT_ORDER.asc) {
    sortOrder = SORT_ORDER.desc;
  }

  let orderColumnFunction;
  switch (columnName) {
    case 'name':
      orderColumnFunction = (pipeline) => pipeline.name.toLowerCase();
      break;
    case 'type':
      orderColumnFunction = (pipeline) => pipeline.artifact.name;
      break;
    case 'status':
      orderColumnFunction = (pipeline) => statusMap[pipeline.name].displayStatus;
      break;
    case 'lastStartTime':
      orderColumnFunction = (pipeline) => {
        const lastStarting = statusMap[pipeline.name].lastStarting;
        if (!lastStarting) {
          return sortOrder === SORT_ORDER.asc ? Infinity : -1;
        }
        return lastStarting;
      };
      break;
    case 'runs':
      orderColumnFunction = (pipeline) => runsCountMap[pipeline.name] || 0;
      break;
  }

  const pipelines = orderBy(state.pipelines, [orderColumnFunction], [sortOrder]);

  Store.dispatch({
    type: Actions.setSort,
    payload: {
      sortColumn: columnName,
      sortOrder,
      pipelines,
    },
  });
}
