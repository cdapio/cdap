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
import Duration from 'components/Duration';
import { GLOBALS } from 'services/global-constants';
import { IPipeline } from 'components/PipelineList/DeployedPipelineView/types';
import IconSVG from 'components/IconSVG';

interface IProps {
  pipeline: IPipeline;
}

export default function NextRun({ pipeline }: IProps) {
  const { nextRuntime, artifact } = pipeline;
  if (nextRuntime === null) {
    return (
      <div className="next-run">
        <span className="fa fa-spin fa-lg">
          <IconSVG name="icon-spinner" />
        </span>
      </div>
    );
  }
  if (
    artifact.name === GLOBALS.etlDataStreams ||
    (Array.isArray(nextRuntime) && !nextRuntime.length)
  ) {
    return (
      <div className="next-run">
        <span>--</span>
      </div>
    );
  }

  let { time: nextRun } = nextRuntime[0];
  nextRun = parseInt(nextRun, 10);
  return (
    <div className="next-run">
      <Duration targetTime={nextRun} />
    </div>
  );
}
