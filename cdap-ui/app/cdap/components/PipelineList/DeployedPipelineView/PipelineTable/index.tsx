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
import { IPipeline } from 'components/PipelineList/DeployedPipelineView/types';
import EmptyList, { VIEW_TYPES } from 'components/PipelineList/EmptyList';
import { Actions } from 'components/PipelineList/DeployedPipelineView/store';
import LoadingSVGCentered from 'components/LoadingSVGCentered';

import './PipelineTable.scss';
import EmptyMessageContainer from 'components/EmptyMessageContainer';

interface IProps {
  pipelines: IPipeline[];
  pipelinesLoading: boolean;
  search: string;
  onClear: () => void;
}

const PREFIX = 'features.PipelineList';

const PipelineTableView: React.SFC<IProps> = ({ pipelines, pipelinesLoading, search, onClear }) => {
  function renderBody() {
    if (pipelinesLoading) {
      return <LoadingSVGCentered />;
    }

    if (pipelines.length === 0) {
      return <EmptyList type={VIEW_TYPES.deployed} />;
    }

    let filteredList = pipelines;
    if (search.length > 0) {
      filteredList = filteredList.filter((pipeline) => {
        const name = pipeline.name.toLowerCase();
        const searchFilter = search.toLowerCase();

        return name.indexOf(searchFilter) !== -1;
      });
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
            <strong>{T.translate(`${PREFIX}.pipelineName`)}</strong>
            <strong>{T.translate(`${PREFIX}.type`)}</strong>
            <strong>{T.translate(`${PREFIX}.status`)}</strong>
            <strong>{T.translate(`${PREFIX}.lastStartTime`)}</strong>
            <strong>{T.translate(`${PREFIX}.nextRun`)}</strong>
            <strong>{T.translate(`${PREFIX}.runs`)}</strong>
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
