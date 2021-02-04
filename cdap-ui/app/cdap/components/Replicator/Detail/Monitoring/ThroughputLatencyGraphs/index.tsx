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

import React, { useEffect, useState, useContext } from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import ThroughputGraph from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs/ThroughputGraph';
import LatencyGraph from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs/LatencyGraph';
import { getCurrentNamespace } from 'services/NamespaceStore';
import MetricsQueryHelper from 'services/MetricsQueryHelper';
import { PROGRAM_INFO } from 'components/Replicator/constants';
import { DetailContext } from 'components/Replicator/Detail';
import { MyMetricApi } from 'api/metric';
import { throughputLatencyParser } from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs/parser';
import { getFullyQualifiedTableName } from 'components/Replicator/utilities';
import isEqual from 'lodash/isEqual';

const styles = (): StyleRules => {
  return {
    root: {
      display: 'grid',
      gridTemplateColumns: '50% 50%',
      paddingBottom: '25px',

      '& > div': {
        padding: '0 15px',
      },
    },
  };
};

interface ITags {
  namespace: string;
  app: string;
  worker: string;
  run: string;
  instance: string;
  ent?: string;
}

const ThroughputLatencyGraphsView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const [data, setData] = useState([]);
  const { name, tables, activeTable, timeRange } = useContext(DetailContext);

  useEffect(() => {
    if (tables.size === 0) {
      return;
    }

    const tags: ITags = {
      namespace: getCurrentNamespace(),
      app: name,
      worker: PROGRAM_INFO.programId,
      run: '*',
      instance: '*',
    };

    if (activeTable) {
      tags.ent = getFullyQualifiedTableName(activeTable);
    }

    const tagsParams = MetricsQueryHelper.tagsToParams(tags);

    const metrics = [
      'inserts',
      'updates',
      'deletes',
      'errors',
      'latency.seconds',
      'data.processed.bytes',
    ]
      .map((metric) => {
        return `metric=user.dml.${metric}`;
      })
      .join('&');

    const start = `start=now-${timeRange}`;
    const end = 'end=now';
    const aggregate = 'aggregate=false';
    const resolution = 'resolution=auto';

    const params = [start, end, aggregate, resolution, tagsParams, metrics].join('&');

    const metricsPoll$ = MyMetricApi.pollQueryTags({ params }).subscribe(
      (res) => {
        const parsedMetrics = throughputLatencyParser(res, tables.size, activeTable);

        if (!isEqual(parsedMetrics, data)) {
          setData(parsedMetrics);
        }
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log('err', err);
      }
    );

    return () => {
      if (metricsPoll$ && typeof metricsPoll$.unsubscribe === 'function') {
        metricsPoll$.unsubscribe();
      }
    };
  }, [tables, activeTable, timeRange]);

  return (
    <div className={classes.root}>
      <ThroughputGraph data={data} />
      <LatencyGraph data={data} />
    </div>
  );
};

const ThroughputLatencyGraphs = withStyles(styles)(ThroughputLatencyGraphsView);
export default ThroughputLatencyGraphs;
