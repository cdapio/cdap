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
import ThroughputGraph from 'components/Replicator/Detail/ThroughputLatencyGraphs/ThroughputGraph';
import LatencyGraph from 'components/Replicator/Detail/ThroughputLatencyGraphs/LatencyGraph';
import { getCurrentNamespace } from 'services/NamespaceStore';
import MetricsQueryHelper from 'services/MetricsQueryHelper';
import { PROGRAM_INFO } from 'components/Replicator/constants';
import { DetailContext } from 'components/Replicator/Detail';
import { MyMetricApi } from 'api/metric';
import { throughputLatencyParser } from 'components/Replicator/Detail/ThroughputLatencyGraphs/parser';

const styles = (): StyleRules => {
  return {
    root: {
      display: 'grid',
      gridTemplateColumns: '50% 50%',

      '& > div': {
        padding: '0 15px',
      },
    },
  };
};

const ThroughputLatencyGraphsView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const [data, setData] = useState([]);
  const { name } = useContext(DetailContext);

  useEffect(() => {
    const tags = {
      namespace: getCurrentNamespace(),
      app: name,
      worker: PROGRAM_INFO.programId,
      run: '*',
      instance: '*',
    };

    const tagsParams = MetricsQueryHelper.tagsToParams(tags);

    const metrics = ['insert', 'update', 'delete', 'error', 'latency.seconds']
      .map((metric) => {
        return `metric=user.dml.${metric}`;
      })
      .join('&');

    const start = `start=now-24h`;
    const end = 'end=now';
    const aggregate = 'aggregate=false';
    const resolution = 'resolution=auto';

    const params = [start, end, aggregate, resolution, tagsParams, metrics].join('&');

    MyMetricApi.queryTags({ params }).subscribe(
      (res) => {
        setData(throughputLatencyParser(res));
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log('err', err);
      }
    );
  }, []);

  return (
    <div className={classes.root}>
      <ThroughputGraph data={data} />
      <LatencyGraph data={data} />
    </div>
  );
};

const ThroughputLatencyGraphs = withStyles(styles)(ThroughputLatencyGraphsView);
export default ThroughputLatencyGraphs;
