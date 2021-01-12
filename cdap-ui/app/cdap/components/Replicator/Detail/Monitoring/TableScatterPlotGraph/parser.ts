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

import { orderBy } from 'natural-orderby';
import { ITableInfo } from 'components/Replicator/types';
import { getFullyQualifiedTableName } from 'components/Replicator/utilities';

export interface ITableMetricsData {
  tableName: string;
  tableInfo: ITableInfo;
  errors: number;
  totalEvents: number;
  eventsPerMin: number;
  latency: number;
  dataReplicated: number;
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
  totalEvents: 0,
  latency: 0,
  dataReplicated: 0,
};

const METRIC_MAP = {
  'user.dml.inserts': 'totalEvents',
  'user.dml.updates': 'totalEvents',
  'user.dml.deletes': 'totalEvents',
  'user.dml.errors': 'errors',
  'user.dml.latency.seconds': 'latency',
  'user.dml.data.processed.bytes': 'dataReplicated',
};

const ONE_MIN_SECONDS = 60;

export function parseTableMetrics(
  rawData: IRawMetricData,
  tableList: ITableInfo[]
): ITableMetricsData[] {
  const tableMap: Record<string, ITableMetricsData> = {};

  // initialize tables
  tableList.forEach((tableInfo) => {
    const tableName = getFullyQualifiedTableName(tableInfo);
    tableMap[tableName] = {
      tableName,
      tableInfo,
      ...INITIAL_DATA,
    };
  });

  rawData.series.forEach((metricSeries) => {
    const metricName = METRIC_MAP[metricSeries.metricName];
    const tableName = Object.values(metricSeries.grouping)[0];

    if (!tableMap[tableName]) {
      return;
    }

    const sumData = metricSeries.data.reduce((prev, curr) => {
      return prev + curr.value;
    }, 0);

    if (metricName === 'latency') {
      // calculate average for latency
      tableMap[tableName][metricName] = sumData / metricSeries.data.length / ONE_MIN_SECONDS;
    } else {
      tableMap[tableName][metricName] += sumData;
    }
  });

  // convert events total to events per minute
  const duration = rawData.endTime - rawData.startTime;
  const durationMinute = duration / ONE_MIN_SECONDS;

  tableList.forEach((tableInfo) => {
    const tableName = getFullyQualifiedTableName(tableInfo);
    if (!tableMap[tableName]) {
      return;
    }
    tableMap[tableName].eventsPerMin = tableMap[tableName].totalEvents / durationMinute;
  });

  const output = orderBy(Object.values(tableMap), [(val) => val.tableName], ['asc']);
  return output;
}
