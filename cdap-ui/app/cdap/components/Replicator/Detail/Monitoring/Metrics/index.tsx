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

import React, { useState, useContext, useEffect } from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { MyMetricApi } from 'api/metric';
import { DetailContext } from 'components/Replicator/Detail';
import { getCurrentNamespace } from 'services/NamespaceStore';
import MetricsQueryHelper from 'services/MetricsQueryHelper';
import { PROGRAM_INFO } from 'components/Replicator/constants';
import {
  parseAggregateMetric,
  INITIAL_OUTPUT,
} from 'components/Replicator/Detail/Monitoring/Metrics/parser';
import { ONE_MIN_SECONDS, truncateNumber } from 'services/helpers';
import { formatNumber } from 'components/Replicator/utilities';

const styles = (theme): StyleRules => {
  return {
    root: {
      marginBottom: '30px',
    },
    grid: {
      display: 'grid',
      gridTemplateColumns: 'repeat(8, 1fr)',
      margin: '0 -40px',
      padding: '0 25px',

      '& > div': {
        padding: '0 15px',
      },
    },
    unit: {
      color: theme.palette.grey[200],

      '& > div': {
        padding: '0 15px',
      },
    },
    header: {
      fontSize: '14px',
      fontWeight: 600,
      backgroundColor: theme.palette.grey[700],
      alignItems: 'end',

      '& > div': {
        padding: '5px 15px',
      },
    },
    metricContent: {
      fontSize: '26px',
    },
    error: {
      color: theme.palette.red[100],
    },
    latencyLabel: {
      fontWeight: 400,
      fontSize: '16px',
      color: theme.palette.grey[100],
    },
    marginRight: {
      marginRight: '5px',
    },
  };
};

const PRECISION = 2;
const THRESHOLD = 999999;

const MetricsView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const { name, timeRange, tables } = useContext(DetailContext);
  const [data, setData] = useState({ ...INITIAL_OUTPUT });

  useEffect(() => {
    const tags = {
      namespace: getCurrentNamespace(),
      app: name,
      worker: PROGRAM_INFO.programId,
      run: '*',
      instance: '*',
    };

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
        setData(parseAggregateMetric(res, tables.size));
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
  }, [timeRange, tables]);

  const parsedLatency = parseLatencySeconds(data.latency);

  return (
    <div className={classes.root}>
      <div className={`${classes.grid} ${classes.header}`}>
        <div>Data replicated</div>
        <div>Throughput</div>
        <div>Avg latency</div>
        <div>Errors</div>
        <div>Total events</div>
        <div>Inserts</div>
        <div>Updates</div>
        <div>Deletes</div>
      </div>
      <div className={`${classes.grid} ${classes.metricContent}`}>
        <div>{data.dataReplicated}</div>
        <div>{truncateNumber(data.eventsPerMin, PRECISION)}</div>
        <div>
          <span>{parsedLatency.minutes}</span>
          <span className={`${classes.latencyLabel} ${classes.marginRight}`}>min</span>
          <span>{parsedLatency.seconds}</span>
          <span className={classes.latencyLabel}>sec</span>
        </div>
        <div className={classes.error}>{data.errors}</div>
        <div>{formatNumber(data.totalEvents, THRESHOLD)}</div>
        <div>{formatNumber(data.inserts, THRESHOLD)}</div>
        <div>{formatNumber(data.updates, THRESHOLD)}</div>
        <div>{formatNumber(data.deletes, THRESHOLD)}</div>
      </div>
      <div className={`${classes.grid} ${classes.unit}`}>
        <div />
        <div>events/min</div>
        <div />
        <div />
        <div />
        <div />
        <div />
        <div />
      </div>
    </div>
  );
};

function parseLatencySeconds(latency) {
  const minutes = Math.floor(latency / ONE_MIN_SECONDS);
  const seconds = latency % ONE_MIN_SECONDS;

  return {
    minutes,
    seconds,
  };
}

const Metrics = withStyles(styles)(MetricsView);
export default Metrics;
