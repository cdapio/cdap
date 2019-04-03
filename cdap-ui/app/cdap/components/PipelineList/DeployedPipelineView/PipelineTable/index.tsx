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
import {
  IPipeline,
  IStatusMap,
  IRunsCountMap,
} from 'components/PipelineList/DeployedPipelineView/types';
import EmptyList, { VIEW_TYPES } from 'components/PipelineList/EmptyList';
import { Actions } from 'components/PipelineList/DeployedPipelineView/store';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import EmptyMessageContainer from 'components/EmptyMessageContainer';
import SortableHeader from 'components/PipelineList/DeployedPipelineView/PipelineTable/SortableHeader';

import './PipelineTable.scss';

interface IProps {
  pipelines: IPipeline[];
  pipelinesLoading: boolean;
  search: string;
  onClear: () => void;
  statusMap: IStatusMap;
  runsCountMap: IRunsCountMap;
  pageLimit: number;
  currentPage: number;
}

const PREFIX = 'features.PipelineList';

const PipelineTableView: React.SFC<IProps> = ({
  pipelines,
  pipelinesLoading,
  search,
  onClear,
  statusMap,
  runsCountMap,
  pageLimit,
  currentPage,
}) => {
  function renderBody() {
    if (pipelinesLoading) {
      return <LoadingSVGCentered />;
    }

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
      const startIndex = (currentPage - 1) * pageLimit;
      const endIndex = startIndex + pageLimit;
      filteredList = pipelines.slice(startIndex, endIndex);
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
          return <PipelineTableRow key={pipeline.name} pipeline={pipeline} />;
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
            <SortableHeader columnName="status" disabled={Object.keys(statusMap).length === 0} />
            <SortableHeader
              columnName="lastStartTime"
              disabled={Object.keys(statusMap).length === 0}
            />
            <strong>{T.translate(`${PREFIX}.nextRun`)}</strong>
            <SortableHeader columnName="runs" disabled={Object.keys(runsCountMap).length === 0} />
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
    pipelines: state.deployed.pipelines,
    pipelinesLoading: state.deployed.pipelinesLoading,
    search: state.deployed.search,
    statusMap: state.deployed.statusMap,
    runsCountMap: state.deployed.runsCountMap,
    pageLimit: state.deployed.pageLimit,
    currentPage: state.deployed.currentPage,
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
