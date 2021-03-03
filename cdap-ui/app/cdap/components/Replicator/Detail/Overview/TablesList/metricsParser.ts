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

import { IRawMetricData, ITableInfo } from 'components/Replicator/types';
import { getFullyQualifiedTableName } from 'components/Replicator/utilities';
import { truncateNumber, ONE_MIN_SECONDS, convertBytesToHumanReadable } from 'services/helpers';

interface IOverviewMetricsData {
  tableName: string;
  inserts: number;
  updates: number;
  deletes: number;
  errors: number;
  latency: number;
  eventsPerMin: number;
  totalEvents: number;
  dataReplicated: number;
}

export const INITIAL_DATA = {
  inserts: 0,
  updates: 0,
  deletes: 0,
  errors: 0,
  latency: 0,
  eventsPerMin: 0,
  totalEvents: 0,
  dataReplicated: 0,
};

const METRIC_MAP = {
  'user.dml.inserts': 'inserts',
  'user.dml.updates': 'updates',
  'user.dml.deletes': 'deletes',
  'user.dml.errors': 'errors',
  'user.dml.latency.seconds': 'latency',
  'user.dml.data.processed.bytes': 'dataReplicated',
};

const PRECISION = 2;

export function parseOverviewMetrics(
  rawData: IRawMetricData,
  tableList: ITableInfo[]
): Record<string, IOverviewMetricsData> {
  const tableMap: Record<string, IOverviewMetricsData> = {};

  // initialize tables
  tableList.forEach((tableInfo) => {
    const tableName = getFullyQualifiedTableName(tableInfo);
    tableMap[tableName] = {
      tableName,
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
    const tableMetrics = tableMap[tableName];
    const totalEvents = tableMetrics.inserts + tableMetrics.updates + tableMetrics.deletes;
    tableMap[tableName].eventsPerMin = truncateNumber(totalEvents / durationMinute, PRECISION);
    tableMap[tableName].totalEvents = truncateNumber(totalEvents);
    tableMap[tableName].latency = truncateNumber(tableMap[tableName].latency, PRECISION);
    tableMap[tableName].dataReplicated = convertBytesToHumanReadable(
      tableMap[tableName].dataReplicated
    );
  });

  return tableMap;
}
