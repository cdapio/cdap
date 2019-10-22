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
import { IPipeline } from 'components/PipelineList/DeployedPipelineView/types';
import { PROGRAM_STATUSES } from 'services/global-constants';
import { objectQuery } from 'services/helpers';

interface IProps {
  pipeline: IPipeline;
}

const Status: React.SFC<IProps> = ({ pipeline }) => {
  const pipelineStatus = objectQuery(pipeline, 'runs', 0, 'status') || PROGRAM_STATUSES.DEPLOYED;
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

export default Status;
