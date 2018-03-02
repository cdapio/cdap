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
import {Provider, connect} from 'react-redux';
import PipelineConfigurationsStore from 'components/PipelineConfigurations/Store';
import PipelineScheduleButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/PipelineScheduleButton';
import PipelineConfigureButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/PipelineConfigureButton';
import PipelineStopButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/PipelineStopButton';
import PipelineRunButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/PipelineRunButton';
import PipelineSummaryButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/PipelineSummaryButton';

const mapStateToConfigureButton = (state, ownProps) => {
  return {
    isBatch: ownProps.isBatch,
    pipelineName: ownProps.pipelineName,
    resolvedMacros: state.resolvedMacros,
    runtimeArgs: state.runtimeArgs
  };
};

const mapStateToRunButton = (state, ownProps) => {
  return {
    isBatch: ownProps.isBatch,
    pipelineName: ownProps.pipelineName,
    runButtonLoading: ownProps.runButtonLoading,
    runError: ownProps.runError,
    runtimeArgs: state.runtimeArgs
  };
};

const mapStateToScheduleButton = (state, ownProps) => {
  return {
    isBatch: ownProps.isBatch,
    pipelineName: ownProps.pipelineName,
    schedule: ownProps.schedule,
    maxConcurrentRuns: ownProps.maxConcurrentRuns,
    scheduleStatus: ownProps.scheduleStatus,
    scheduleButtonLoading: ownProps.scheduleButtonLoading,
    scheduleError: ownProps.scheduleError,
    runtimeArgs: state.runtimeArgs
  };
};

const ConnectedConfigureButton = connect(mapStateToConfigureButton)(PipelineConfigureButton);
const ConnectedRunButton = connect(mapStateToRunButton)(PipelineRunButton);
const ConnectedScheduleButton = connect(mapStateToScheduleButton)(PipelineScheduleButton);

export default function PipelineDetailsButtons({isBatch, pipelineName, schedule, maxConcurrentRuns, scheduleStatus, runs, runButtonLoading, runError, scheduleButtonLoading, scheduleError, stopButtonLoading, stopError}) {
  return (
    <Provider store={PipelineConfigurationsStore}>
      <div className="pipeline-details-buttons">
        <ConnectedConfigureButton
          isBatch={isBatch}
          pipelineName={pipelineName}
        />
        <ConnectedScheduleButton
          isBatch={isBatch}
          pipelineName={pipelineName}
          schedule={schedule}
          maxConcurrentRuns={maxConcurrentRuns}
          scheduleStatus={scheduleStatus}
          scheduleButtonLoading={scheduleButtonLoading}
          scheduleError={scheduleError}
        />
        <PipelineStopButton
          isBatch={isBatch}
          pipelineName={pipelineName}
          runs={runs}
          stopButtonLoading={stopButtonLoading}
          stopError={stopError}
        />
        <ConnectedRunButton
          isBatch={isBatch}
          pipelineName={pipelineName}
          runButtonLoading={runButtonLoading}
          runError={runError}
        />
        <PipelineSummaryButton
          isBatch={isBatch}
          pipelineName={pipelineName}
        />
      </div>
    </Provider>
  );
}

PipelineDetailsButtons.propTypes = {
  isBatch: PropTypes.boolean,
  pipelineName: PropTypes.string,
  schedule: PropTypes.string,
  maxConcurrentRuns: PropTypes.number,
  scheduleStatus: PropTypes.string,
  runs: PropTypes.array,
  currentRun: PropTypes.object,
  runButtonLoading: PropTypes.bool,
  runError: PropTypes.string,
  scheduleButtonLoading: PropTypes.bool,
  scheduleError: PropTypes.string,
  stopButtonLoading: PropTypes.bool,
  stopError: PropTypes.string,
};
