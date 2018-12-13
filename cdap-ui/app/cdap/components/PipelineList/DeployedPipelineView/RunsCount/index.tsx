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
import { connect } from 'react-redux';
import { IRunsCountMap, IPipeline } from 'components/PipelineList/DeployedPipelineView/types';

interface IProps {
  runsCountMap: IRunsCountMap;
  pipeline: IPipeline;
}

const RunsCountView: React.SFC<IProps> = ({ runsCountMap, pipeline }) => {
  const runsCount = runsCountMap[pipeline.name] || 0;

  return <div className="runs">{runsCount}</div>;
};

const mapStateToProps = (state, ownProp) => {
  return {
    runsCountMap: state.deployed.runsCountMap,
    pipeline: ownProp.pipeline,
  };
};

const RunsCount = connect(mapStateToProps)(RunsCountView);

export default RunsCount;
