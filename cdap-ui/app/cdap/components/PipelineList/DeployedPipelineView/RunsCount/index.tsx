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
import { IApplicationRecord } from 'components/PipelineList/DeployedPipelineView/types';
import { getProgram } from 'components/PipelineList/DeployedPipelineView/graphqlHelper';
import { objectQuery } from 'services/helpers';

interface IProps {
  pipeline: IApplicationRecord;
}

const RunsCountView: React.SFC<IProps> = ({ pipeline }) => {
  const runsCount = getRunsCount(pipeline);

  return <div className="runs">{runsCount}</div>;
};

function getRunsCount(pipeline) {
  const program = getProgram(pipeline);
  const totalRuns = objectQuery(program, 'totalRuns');

  return totalRuns || 0;
}

const RunsCount = RunsCountView;

export default RunsCount;
