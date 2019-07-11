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
import IconSVG from 'components/IconSVG';
import StatusMapper from 'services/StatusMapper';
import { objectQuery } from 'services/helpers';
import { IApplicationRecord } from 'components/PipelineList/DeployedPipelineView/types';
import { GLOBALS } from 'services/global-constants';

interface IProps {
  pipeline: IApplicationRecord;
}

const StatusView: React.SFC<IProps> = ({ pipeline }) => {
  const run = getLatestRun(pipeline) || { status: 'DEPLOYED' };
  const pipelineStatus = run.status;
  const displayStatus = StatusMapper.statusMap[pipelineStatus];
  const statusClassName = StatusMapper.getStatusIndicatorClass(displayStatus);

  return (
    <div className="status">
      <span className={`fa fa-fw ${statusClassName}`}>
        <IconSVG name="icon-circle" />
      </span>
      <span className="text">{displayStatus}</span>
    </div>
  );
};

function getLatestRun(pipeline) {
  const applicationDetail = objectQuery(pipeline, 'applicationDetail');

  if (applicationDetail === null || applicationDetail === undefined) {
    return [];
  }

  const programs = objectQuery(applicationDetail, 'programs');

  if (programs === null || programs === undefined) {
    return [];
  }

  const artifact = objectQuery(pipeline, 'artifact');

  if (artifact === null || artifact === undefined) {
    return [];
  }

  const programName = GLOBALS.programInfo[artifact.name].programName;
  const runProgram = programs.find((program) => program.name === programName);

  return runProgram.runs[0];
}

const Status = StatusView;

export default Status;
