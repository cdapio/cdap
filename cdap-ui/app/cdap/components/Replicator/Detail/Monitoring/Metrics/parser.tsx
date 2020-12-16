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

import { IRawMetricData } from 'components/Replicator/types';
import { truncateNumber } from 'services/helpers';
import { convertBytesToHumanReadable } from 'services/helpers';
import { ONE_MIN_SECONDS } from 'services/helpers';

interface IAggregateMetrics {
  dataReplicated: number;
  eventsPerMin: number;
  latency: number;
  errors: number;
  totalEvents: number;
  inserts: number;
  updates: number;
  deletes: number;
}

const METRIC_NAME_MAP = {
  'user.dml.inserts': 'inserts',
  'user.dml.updates': 'updates',
  'user.dml.deletes': 'deletes',
  'user.dml.errors': 'errors',
  'user.dml.latency.seconds': 'latency',
  'user.dml.data.processed.bytes': 'dataReplicated',
};

const PRECISION = 2;

export const INITIAL_OUTPUT = {
  dataReplicated: 0,
  eventsPerMin: 0,
  latency: 0,
  errors: 0,
  totalEvents: 0,
  inserts: 0,
  updates: 0,
  deletes: 0,
};

export function parseAggregateMetric(
  rawData: IRawMetricData,
  numTables: number
): IAggregateMetrics {
  const output = {
    ...INITIAL_OUTPUT,
  };

  rawData.series.forEach((metricSeries) => {
    const metricName = METRIC_NAME_MAP[metricSeries.metricName];

    const sumData = metricSeries.data.reduce((prev, curr) => {
      return prev + curr.value;
    }, 0);

    if (metricName === 'latency') {
      // calculate average for latency
      output[metricName] += sumData / metricSeries.data.length;
    } else {
      output[metricName] += sumData;
    }
  });

  // convert events total to events per minute
  const duration = rawData.endTime - rawData.startTime;
  const durationMinute = duration / ONE_MIN_SECONDS;

  const totalEvents = output.inserts + output.updates + output.deletes;
  output.totalEvents = totalEvents;
  output.eventsPerMin = truncateNumber(totalEvents / durationMinute, PRECISION);
  output.latency = Math.round(output.latency / numTables);
  output.dataReplicated = convertBytesToHumanReadable(output.dataReplicated);

  return output;
}
