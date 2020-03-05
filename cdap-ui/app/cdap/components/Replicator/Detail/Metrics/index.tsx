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

import * as React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { detailContextConnect, IDetailContext } from 'components/Replicator/Detail';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { PROGRAM_INFO } from 'components/Replicator/constants';
import MetricsQueryHelper from 'services/MetricsQueryHelper';
import { MyReplicatorApi } from 'api/replicator';
import Heading, { HeadingTypes } from 'components/Heading';

const styles = (theme): StyleRules => {
  return {
    root: {
      display: 'grid',
      gridTemplateColumns: '33.33% 33.33% 33.33%',
    },
    metricCard: {
      textAlign: 'center',
      border: `1px solid ${theme.palette.grey[200]}`,
      fontSize: '32px',
      padding: '15px',
    },
  };
};

function aggregateValue(seriesData) {
  return seriesData.reduce((acc, curr) => acc + curr.value, 0);
}

const MetricsView: React.FC<IDetailContext & WithStyles<typeof styles>> = ({
  classes,
  targetPluginInfo,
  name,
}) => {
  const [inserts, setInserts] = React.useState(0);
  const [updates, setUpdates] = React.useState(0);
  const [deletes, setDeletes] = React.useState(0);

  const updateMap = {
    insert: setInserts,
    update: setUpdates,
    delete: setDeletes,
  };

  React.useEffect(() => {
    if (!targetPluginInfo) {
      return;
    }

    const tags = {
      namespace: getCurrentNamespace(),
      app: name,
      worker: PROGRAM_INFO.programId,
    };

    const tagsParams = MetricsQueryHelper.tagsToParams(tags);

    const targetStageName = targetPluginInfo.name;
    const metrics = ['insert', 'update', 'delete']
      .map((metric) => {
        return `metric=user.${targetStageName}.dml.${metric}`;
      })
      .join('&');

    const start = 'start=now-24h';
    const end = 'end=now';
    const aggregate = 'aggregate=false';
    const resolution = 'resolution=auto';

    const queryParams = [start, end, aggregate, resolution, tagsParams, metrics].join('&');

    // TODO: optimize polling
    // Don't poll when status is not running - only do a single request
    MyReplicatorApi.pollMetrics({ queryParams }).subscribe(
      (res) => {
        res.series.forEach((metric) => {
          const value = aggregateValue(metric.data);
          const metricName = metric.metricName.split('.');
          const metricType = metricName[metricName.length - 1];

          updateMap[metricType](value);
        });
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log('error', err);
      }
    );
  }, [targetPluginInfo]);

  return (
    <div className={classes.root}>
      <div className={classes.metricCard}>
        <Heading type={HeadingTypes.h4} label="Inserts" />
        <div>{inserts}</div>
      </div>

      <div className={classes.metricCard}>
        <Heading type={HeadingTypes.h4} label="Updates" />
        <div>{updates}</div>
      </div>

      <div className={classes.metricCard}>
        <Heading type={HeadingTypes.h4} label="Deletes" />
        <div>{deletes}</div>
      </div>
    </div>
  );
};

const StyledMetrics = withStyles(styles)(MetricsView);
const Metrics = detailContextConnect(StyledMetrics);
export default Metrics;
