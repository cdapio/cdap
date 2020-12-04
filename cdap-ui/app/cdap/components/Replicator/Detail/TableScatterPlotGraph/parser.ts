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

export interface ITableMetricsData {
  tableName: string;
  errors: number;
  eventsPerMin: number;
  latency: number;
}

interface IMetricData {
  time: number;
  value: number;
}

interface IMetricSeries {
  metricName: string;
  data: IMetricData[];
  grouping: Record<string, string>;
}

interface IRawMetricData {
  startTime: number;
  endTime: number;
  resolution: string;
  series: IMetricSeries[];
}

const INITIAL_DATA = {
  errors: 0,
  eventsPerMin: 0,
  latency: 0,
};

const METRIC_MAP = {
  'user.dml.insert': 'eventsPerMin',
  'user.dml.update': 'eventsPerMin',
  'user.dml.delete': 'eventsPerMin',
  'user.dml.error': 'errors',
  'user.dml.latency.seconds': 'latency',
};

export function parseTableMetrics(
  rawData: IRawMetricData,
  tableList: string[]
): ITableMetricsData[] {
  const tableMap: Record<string, ITableMetricsData> = {};

  // initialize tables
  tableList.forEach((tableName) => {
    tableMap[tableName] = {
      tableName,
      ...INITIAL_DATA,
    };
  });

  rawData.series.forEach((metricSeries) => {
    const metricName = METRIC_MAP[metricSeries.metricName];
    const tableName = Object.values(metricSeries.grouping)[0];

    const sumData = metricSeries.data.reduce((prev, curr) => {
      return prev + curr.value;
    }, 0);

    if (metricName === 'latency') {
      // calculate average for latency
      tableMap[tableName][metricName] = sumData / metricSeries.data.length / 60;
    } else {
      tableMap[tableName][metricName] += sumData;
    }
  });

  // convert events total to events per minute
  const ONE_DAY_MINUTE = 24 * 60;
  tableList.forEach((tableName) => {
    tableMap[tableName].eventsPerMin = tableMap[tableName].eventsPerMin / ONE_DAY_MINUTE;
  });

  const output = Object.values(tableMap);
  return output;
}
