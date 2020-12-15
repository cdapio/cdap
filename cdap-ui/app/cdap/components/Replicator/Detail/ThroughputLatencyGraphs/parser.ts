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

import moment from 'moment';

export interface IThroughputLatencyData {
  time: number;
  inserts: number;
  updates: number;
  deletes: number;
  errors: number;
  latency: number;
  formattedTimeRange: string;
}

interface IMetricData {
  time: number;
  value: number;
}

interface IMetricSeries {
  metricName: string;
  data: IMetricData[];
}

interface IRawMetricData {
  startTime: number;
  endTime: number;
  resolution: string;
  series: IMetricSeries[];
}

const METRIC_NAME_MAP = {
  'user.dml.insert': 'inserts',
  'user.dml.update': 'updates',
  'user.dml.delete': 'deletes',
  'user.dml.error': 'errors',
  'user.dml.latency.seconds': 'latency',
};

const INITIAL_DATA = {
  inserts: 0,
  updates: 0,
  deletes: 0,
  errors: 0,
  latency: 0,
  formattedTimeRange: '',
};

export function throughputLatencyParser(rawData: IRawMetricData): IThroughputLatencyData[] {
  /**
   * <time>: {
   *    time: <value>,
   *    inserts: <value>,
   *    updates: <value>,
   *    deletes: <value>,
   *    errors: <value>,
   *    latency: <value>
   * }
   */
  const timeMap: Record<string, IThroughputLatencyData> = {};

  rawData.series.forEach((metricSeries) => {
    const metricType = METRIC_NAME_MAP[metricSeries.metricName];

    metricSeries.data.forEach((metricData) => {
      const time = metricData.time;
      if (!timeMap[time]) {
        timeMap[time] = {
          time,
          ...INITIAL_DATA,
        };
      }

      timeMap[time][metricType] = metricData.value;
    });
  });

  // Interpolate data with missing times
  const resolution = parseInt(rawData.resolution, 10);
  const startTime = rawData.startTime - (rawData.startTime % resolution);
  let currentTime = startTime;
  while (currentTime < rawData.endTime) {
    if (!timeMap[currentTime]) {
      timeMap[currentTime] = {
        time: currentTime,
        ...INITIAL_DATA,
      };
    }

    currentTime += resolution;
  }

  const output = Object.values(timeMap).map((outputData) => {
    const time = outputData.time * 1000;
    const startRange = moment(time).format('MM/DD - hh:mmA');
    const endRange = moment(time)
      .add(1, 'h')
      .format('hh:mmA');

    return {
      ...outputData,
      time,
      formattedTimeRange: `${startRange} - ${endRange}`,
    };
  });

  return output;
}
