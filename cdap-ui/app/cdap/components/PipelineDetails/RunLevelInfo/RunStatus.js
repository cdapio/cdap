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

import React from 'react';
import PropTypes from 'prop-types';
import {connect} from 'react-redux';
import StatusMapper from 'services/StatusMapper';
import IconSVG from 'components/IconSVG';
import RunningRunsPopover from 'components/PipelineDetails/RunLevelInfo/RunningRunsPopover';

const mapStateToProps = (state) => {
  return {
    runs: state.runs,
    currentRun: state.currentRun,
    pipelineId: state.name
  };
};

const RunStatus = ({runs, currentRun, pipelineId}) => {
  let status;
  if (currentRun && currentRun.status) {
    status = currentRun.status;
  } else {
    status = 'DEPLOYED';
  }
  status = StatusMapper.lookupDisplayStatus(status);
  let statusCSSClass = StatusMapper.getStatusIndicatorClass(status);

  let runningRuns = runs.filter(run => run.status === 'RUNNING');

  return (
    <div className="run-info-container run-status-container">
      <div>
        <strong>Status</strong>
      </div>
      <span className={`run-status-bubble ${statusCSSClass}`}>
        <IconSVG name="icon-circle" />
      </span>
      <span>{status}</span>
      {
        runningRuns.length > 1 ?
          <RunningRunsPopover
            runs={runningRuns}
            currentRunId={currentRun.runid}
            pipelineId={pipelineId}
          />
        :
          null
      }
    </div>
  );
};

RunStatus.propTypes = {
  runs: PropTypes.array,
  currentRun: PropTypes.object,
  pipelineId: PropTypes.string
};

const ConnectedRunStatus = connect(mapStateToProps)(RunStatus);
export default ConnectedRunStatus;
