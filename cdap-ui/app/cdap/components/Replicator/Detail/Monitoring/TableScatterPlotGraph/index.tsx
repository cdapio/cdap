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
import { parseTableMetrics } from 'components/Replicator/Detail/Monitoring/TableScatterPlotGraph/parser';
import {
  COLOR_MAP,
  renderScatterPlot,
} from 'components/Replicator/Detail/Monitoring/TableScatterPlotGraph/scatterPlot';
import ChartContainer from 'components/ChartContainer';
import Heading, { HeadingTypes } from 'components/Heading';
import ChartTableSwitcher from 'components/Replicator/Detail/ChartTableSwitcher';
import ScatterPlotTable from 'components/Replicator/Detail/Monitoring/TableScatterPlotGraph/ScatterPlotTable';
import ScatterPlotTooltip from 'components/Replicator/Detail/Monitoring/TableScatterPlotGraph/ScatterPlotTooltip';

const styles = (): StyleRules => {
  return {
    root: {
      marginBottom: '25px',
      position: 'relative',
    },
    headingSection: {
      marginBottom: '10px',
    },
    bottomLegend: {
      display: 'flex',
      paddingLeft: '30px',

      '& > div': {
        marginRight: '15px',
        width: '65px',
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
    errorTable: {
      backgroundColor: COLOR_MAP.error,
      borderColor: COLOR_MAP.errorOutline,
    },
  };
};

const CONTAINER_ID = 'replication-scatter-plot';

const TableScatterPlotGraphView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  const [data, setData] = useState([]);
  const { name, tables, activeTable, setActiveTable, timeRange } = useContext(DetailContext);

  function onClick(d) {
    const selectedTable = d ? d.tableInfo : null;
    setActiveTable(selectedTable);
  }

  function renderTooltip(tooltip) {
    return <ScatterPlotTooltip tooltip={tooltip} />;
  }

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
    const groupBy = 'groupBy=ent';

    const params = [start, end, aggregate, groupBy, tagsParams, metrics].join('&');

    MyMetricApi.queryTags({ params }).subscribe(
      (res) => {
        setData(parseTableMetrics(res, tables.toList().toJS()));
      },
      (err) => {
        // tslint:disable-next-line: no-console
        console.log('err', err);
      }
    );
  }, [tables, timeRange]);

  const chart = (
    <React.Fragment>
      <ChartContainer
        containerId={CONTAINER_ID}
        data={data}
        chartRenderer={renderScatterPlot.bind(this, activeTable)}
        watchWidth={true}
        renderTooltip={renderTooltip}
        onClick={onClick}
        additionalWatchProperty={[activeTable]}
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
        <div>
          <div className={`${classes.circle} ${classes.errorTable}`} />
          <div>Table with errors</div>
        </div>
      </div>
    </React.Fragment>
  );

  const table = <ScatterPlotTable data={data} />;

  return (
    <div className={classes.root}>
      <div className={classes.headingSection}>
        <div>
          <Heading type={HeadingTypes.h4} label="Tables performance" />
        </div>
        <div>Latency and throughput by table - select one to view details below</div>
      </div>
      <ChartTableSwitcher chart={chart} table={table} />
    </div>
  );
};

const TableScatterPlotGraph = withStyles(styles)(TableScatterPlotGraphView);
export default TableScatterPlotGraph;
