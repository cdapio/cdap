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
import { humanReadableDate } from 'services/helpers';
import { IStatusMap, IPipeline } from 'components/PipelineList/DeployedPipelineView/types';

interface ILastStartViewProps {
  statusMap: IStatusMap;
  pipeline: IPipeline;
}

const LastStartView: React.SFC<ILastStartViewProps> = ({ statusMap, pipeline }) => {
  const pipelineStatus = statusMap[pipeline.name] || {};
  const lastStarting = pipelineStatus.lastStarting;

  return <div className="table-column last-start">{humanReadableDate(lastStarting)}</div>;
};

const mapStateToProps = (state, ownProp) => {
  return {
    statusMap: state.deployed.statusMap,
    pipeline: ownProp.pipeline,
  };
};

const LastStart = connect(mapStateToProps)(LastStartView);

export default LastStart;
