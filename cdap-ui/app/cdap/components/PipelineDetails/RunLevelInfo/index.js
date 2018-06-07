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
import {Provider, connect} from 'react-redux';
import PipelineDetailStore from 'components/PipelineDetails/store';
import CurrentRunIndex from 'components/PipelineDetails/RunLevelInfo/CurrentRunIndex';
import RunConfigs from 'components/PipelineDetails/RunLevelInfo/RunConfigs';
import RunLogs from 'components/PipelineDetails/RunLevelInfo/RunLogs';
import RunStatus from 'components/PipelineDetails/RunLevelInfo/RunStatus';
import RunStartTime from 'components/PipelineDetails/RunLevelInfo/RunStartTime';
import RunDuration from 'components/PipelineDetails/RunLevelInfo/RunDuration';
import RunNumWarnings from 'components/PipelineDetails/RunLevelInfo/RunNumWarnings';
import RunNumErrors from 'components/PipelineDetails/RunLevelInfo/RunNumErrors';
import RunComputeProfile from 'components/PipelineDetails/RunLevelInfo/RunComputeProfile';
import {GLOBALS} from 'services/global-constants';

require('./RunLevelInfo.scss');

const mapStateToProps = (state) => {
  return {
    currentRun: state.currentRun,
    runs: state.runs,
    isBatch: state.artifact.name === GLOBALS.etlDataPipeline,
    pipelineName: state.name
  };
};

const ConnectedRunConfigs = connect(mapStateToProps)(RunConfigs);
const ConnectedRunNumWarnings = connect(mapStateToProps)(RunNumWarnings);
const ConnectedRunNumErrors = connect(mapStateToProps)(RunNumErrors);

export default function RunLevelInfo() {
  return (
    <Provider store={PipelineDetailStore}>
      <div className="pipeline-details-run-level-info">
        <CurrentRunIndex />
        <ConnectedRunConfigs />
        <RunLogs />
        <RunStatus />
        <RunStartTime />
        <RunDuration />
        <ConnectedRunNumWarnings />
        <ConnectedRunNumErrors />
        <RunComputeProfile />
      </div>
    </Provider>
  );
}
