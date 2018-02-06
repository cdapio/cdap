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
import {GLOBALS} from 'services/global-constants';
import ScheduleButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/ScheduleButton';
import ConfigureButton from 'components/PipelineDetails/PipelineDetailsTopPanel/PipelineDetailsButtons/ConfigureButton';

const mapStateToScheduleButton = (state) => {
  return {
    isBatch: state.artifact.name === GLOBALS.etlDataPipeline,
    schedule: state.config.schedule,
    maxConcurrentRuns: state.config.maxConcurrentRuns,
    pipelineName: state.name,
    scheduleStatus: state.scheduleStatus
  };
};

const mapStateToConfigureButton = (state) => {
  return {
    isBatch: state.artifact.name === GLOBALS.etlDataPipeline,
    pipelineName: state.name,
    config: state.config
  };
};

const ConnectedScheduleButton = connect(mapStateToScheduleButton, null)(ScheduleButton);
const ConnectedConfigureButton = connect(mapStateToConfigureButton, null)(ConfigureButton);

export default function PipelineDetailsButtons() {
  return (
    <Provider store={PipelineDetailStore}>
      <div className="pipeline-details-buttons">
        <ConnectedScheduleButton />
        <ConnectedConfigureButton />
      </div>
    </Provider>
  );
}
