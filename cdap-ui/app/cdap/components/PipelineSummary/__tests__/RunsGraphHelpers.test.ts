/*
 * Copyright © 2020 Cask Data, Inc.
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

import { getResolution } from 'components/PipelineSummary/RunsGraphHelpers';
const secondsi18nLabel = 'features.PipelineSummary.pipelineNodesMetricsGraph.seconds';
const minutesi18nLabel = 'features.PipelineSummary.pipelineNodesMetricsGraph.minutes';
const hoursi18nLabel = 'features.PipelineSummary.pipelineNodesMetricsGraph.hours';
describe('Resolving resolution in plugin metrics graph', () => {
  it('Should return default if no input is provided', () => {
    expect(getResolution('')).toBe(secondsi18nLabel);
  });
  it('Should return seconds if resolution is < 60s', () => {
    expect(getResolution('1s')).toBe(secondsi18nLabel);
    expect(getResolution('20s')).toBe(secondsi18nLabel);
    expect(getResolution('59s')).toBe(secondsi18nLabel);
  });
  it('Should return minutes if resolution is between 60 and 3600s', () => {
    expect(getResolution('60s')).toBe(minutesi18nLabel);
    expect(getResolution('2300s')).toBe(minutesi18nLabel);
    expect(getResolution('2599s')).toBe(minutesi18nLabel);
  });

  it('Should return hours if resolution is > 3600s', () => {
    expect(getResolution('3600s')).toBe(hoursi18nLabel);
    expect(getResolution('6000s')).toBe(hoursi18nLabel);
  });

  it('Should not error out if given an invalid resolution', () => {
    expect(getResolution('unknown')).toBe(secondsi18nLabel);
    expect(getResolution(10 as any)).toBe(secondsi18nLabel);
    expect(getResolution('-100s')).toBe(secondsi18nLabel);
  });
});
