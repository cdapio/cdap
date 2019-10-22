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

import * as React from 'react';
import PipelineTableRow from 'components/PipelineList/DeployedPipelineView/PipelineTable/PipelineTableRow';
import { connect } from 'react-redux';
import T from 'i18n-react';
import { PROGRAM_STATUSES } from 'services/global-constants';
import { IPipeline } from 'components/PipelineList/DeployedPipelineView/types';
import EmptyList, { VIEW_TYPES } from 'components/PipelineList/EmptyList';
import { Actions, SORT_ORDER } from 'components/PipelineList/DeployedPipelineView/store';
import EmptyMessageContainer from 'components/EmptyMessageContainer';
import SortableHeader from 'components/PipelineList/DeployedPipelineView/PipelineTable/SortableHeader';
import orderBy from 'lodash/orderBy';
import './PipelineTable.scss';
import { objectQuery } from 'services/helpers';

interface IProps {
  pipelines: IPipeline[];
  search: string;
  onClear: () => void;
  pageLimit: number;
  currentPage: number;
  sortOrder: string;
  sortColumn: string;
  refetch: () => void;
}

const PREFIX = 'features.PipelineList';

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

const PipelineTableView: React.SFC<IProps> = ({
  pipelines,
  search,
  onClear,
  pageLimit,
  currentPage,
  sortOrder,
  sortColumn,
  refetch,
}) => {
  function renderBody() {
    if (pipelines.length === 0) {
      return <EmptyList type={VIEW_TYPES.deployed} />;
    }

    let filteredList = pipelines;
    if (search.length > 0) {
      filteredList = pipelines.filter((pipeline) => {
        const name = pipeline.name.toLowerCase();
        const searchFilter = search.toLowerCase();

        return name.indexOf(searchFilter) !== -1;
      });
    } else {
      filteredList = orderBy(
        filteredList,
        [getOrderColumnFunction(sortColumn, sortOrder)],
        [sortOrder]
      );

      const startIndex = (currentPage - 1) * pageLimit;
      const endIndex = startIndex + pageLimit;
      filteredList = filteredList.slice(startIndex, endIndex);
    }

    if (filteredList.length === 0) {
      return (
        <EmptyMessageContainer
          title={T.translate(`${PREFIX}.EmptyList.EmptySearch.heading`, { search }).toString()}
        >
          <ul>
            <li>
              <span className="link-text" onClick={onClear}>
                {T.translate(`${PREFIX}.EmptyList.EmptySearch.clear`)}
              </span>
              <span>{T.translate(`${PREFIX}.EmptyList.EmptySearch.search`)}</span>
            </li>
          </ul>
        </EmptyMessageContainer>
      );
    }

    return (
      <div className="grid-body">
        {filteredList.map((pipeline) => {
          return <PipelineTableRow key={pipeline.name} pipeline={pipeline} refetch={refetch} />;
        })}
      </div>
    );
  }

  return (
    <div className="grid-wrapper pipeline-list-table">
      <div className="grid grid-container">
        <div className="grid-header">
          <div className="grid-row">
            <SortableHeader columnName="name" />
            <SortableHeader columnName="type" />
            <SortableHeader columnName="status" />
            <SortableHeader columnName="lastStartTime" />
            <strong>{T.translate(`${PREFIX}.nextRun`)}</strong>
            <SortableHeader columnName="runs" />
            <strong>{T.translate(`${PREFIX}.tags`)}</strong>
            <strong />
          </div>
        </div>

        {renderBody()}
      </div>
    </div>
  );
};

const mapStateToProps = (state) => {
  return {
    search: state.deployed.search,
    pageLimit: state.deployed.pageLimit,
    currentPage: state.deployed.currentPage,
    sortOrder: state.deployed.sortOrder,
    sortColumn: state.deployed.sortColumn,
  };
};

const mapDispatch = (dispatch) => {
  return {
    onClear: () => {
      dispatch({
        type: Actions.setSearch,
        payload: {
          search: '',
        },
      });
    },
  };
};

const PipelineTable = connect(
  mapStateToProps,
  mapDispatch
)(PipelineTableView);

export default PipelineTable;
