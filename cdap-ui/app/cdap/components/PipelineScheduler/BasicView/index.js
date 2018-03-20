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
import IntervalOption from 'components/PipelineScheduler/BasicView/IntervalOption';
import RepeatEvery from 'components/PipelineScheduler/BasicView/RepeatEvery';
import StartingAt from 'components/PipelineScheduler/BasicView/StartingAt';
import MaxConcurrentRuns from 'components/PipelineScheduler/BasicView/MaxConcurrentRuns';
import Summary from 'components/PipelineScheduler/BasicView/Summary';

export default function BasicView() {
  return (
    <div className="schedule-type-content">
      <IntervalOption />
      <RepeatEvery />
      <StartingAt />
      <MaxConcurrentRuns />
      <Summary />
    </div>
  );
}
