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
import { IPipeline } from 'components/PipelineList/DeployedPipelineView/types';
import IconSVG from 'components/IconSVG';

interface IProps {
  pipeline: IPipeline;
}

const RunsCount: React.SFC<IProps> = ({ pipeline }) => {
  const runsCount = pipeline.totalRuns;
  if (runsCount === null) {
    return (
      <div className="runs">
        <span className="fa fa-spin fa-lg">
          <IconSVG name="icon-spinner" />
        </span>
      </div>
    );
  }

  if (runsCount === 0) {
    return <div className="runs">--</div>;
  }

  return <div className="runs">{runsCount}</div>;
};

export default RunsCount;
