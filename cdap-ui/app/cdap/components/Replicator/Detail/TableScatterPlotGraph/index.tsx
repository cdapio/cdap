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
import { MyMetricApi } from 'api/metric';
import { DetailContext } from 'components/Replicator/Detail';
import { getCurrentNamespace } from 'services/NamespaceStore';
import MetricsQueryHelper from 'services/MetricsQueryHelper';
import { PROGRAM_INFO } from 'components/Replicator/constants';
import { parseTableMetrics } from 'components/Replicator/Detail/TableScatterPlotGraph/parser';
import {
  COLOR_MAP,
  renderScatterPlot,
} from 'components/Replicator/Detail/TableScatterPlotGraph/scatterPlot';
import ChartContainer from 'components/ChartContainer';
import Heading, { HeadingTypes } from 'components/Heading';

const styles = (): StyleRules => {
  return {
    root: {
      marginBottom: '25px',
    },
    headingSection: {
      marginBottom: '10px',
    },
    bottomLegend: {
      display: 'flex',
      paddingLeft: '30px',

      '& > div': {
        marginRight: '25px',
        width: '50px',
        textAlign: 'center',
      },
    },
    circle: {
      display: 'inline-block',
      height: '20px',
      width: '20px',
      border: '1px solid',
      borderRadius: '50%',
    },
    activeTable: {
      backgroundColor: COLOR_MAP.active,
      borderColor: COLOR_MAP.activeOutline,
    },
    inactiveTable: {
      backgroundColor: COLOR_MAP.inactive,
      borderColor: COLOR_MAP.inactiveOutline,
    },
  };
};

const CONTAINER_ID = 'replication-scatter-plot';

const TableScatterPlotGraphView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const [data, setData] = useState([]);
  const { name, tables } = useContext(DetailContext);

  useEffect(() => {
    if (tables.size === 0) {
      return;
    }

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
    const groupBy = 'groupBy=ent';

    const params = [start, end, aggregate, groupBy, tagsParams, metrics].join('&');

    MyMetricApi.queryTags({ params }).subscribe(
      (res) => {
        setData(
          parseTableMetrics(
            res,
            tables
              .toList()
              .map((tableInfo) => tableInfo.get('table'))
              .toJS()
          )
        );
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log('err', err);
      }
    );
  }, [tables]);

  return (
    <div className={classes.root}>
      <div className={classes.headingSection}>
        <div>
          <Heading type={HeadingTypes.h4} label="Tables performance" />
        </div>
        <div>Latency and throughput by table - select one to view details below</div>
      </div>
      <ChartContainer
        containerId={CONTAINER_ID}
        data={data}
        chartRenderer={renderScatterPlot}
        watchWidth={true}
      />
      <div className={classes.bottomLegend}>
        <div>
          <div className={`${classes.circle} ${classes.activeTable}`} />
          <div>Active table</div>
        </div>
        <div>
          <div className={`${classes.circle} ${classes.inactiveTable}`} />
          <div>Inactive table</div>
        </div>
      </div>
    </div>
  );
};

const TableScatterPlotGraph = withStyles(styles)(TableScatterPlotGraphView);
export default TableScatterPlotGraph;
