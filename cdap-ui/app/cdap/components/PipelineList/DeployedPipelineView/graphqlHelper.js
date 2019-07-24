/*
 * Copyright © 2019 Cask Data, Inc.
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

import { objectQuery } from 'services/helpers';
import { GLOBALS } from 'services/global-constants';

function getProgram(pipeline) {
  const programs = objectQuery(pipeline, 'applicationDetail', 'programs');

  if (!programs) {
    return {};
  }

  const artifact = objectQuery(pipeline, 'artifact');

  if (!artifact) {
    return {};
  }

  const programName = GLOBALS.programInfo[artifact.name].programName;

  return programs.find((program) => program.name === programName);
}

function getProgramRuns(program) {
  const runs = objectQuery(program, 'runs');

  return runs || [];
}

function getLatestRun(pipeline) {
  const program = getProgram(pipeline);
  const runs = getProgramRuns(program);

  if (runs.length === 0) {
    return null;
  }

  return runs[0];
}

export { getProgram, getProgramRuns, getLatestRun };
