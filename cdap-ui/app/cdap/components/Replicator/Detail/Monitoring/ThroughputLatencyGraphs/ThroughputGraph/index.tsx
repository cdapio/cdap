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

import React from 'react';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import {
  COLOR_MAP,
  renderThroughputGraph,
} from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs/ThroughputGraph/throughput';
import Heading, { HeadingTypes } from 'components/Heading';
import ChartContainer from 'components/ChartContainer';
import { IThroughputLatencyData } from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs/parser';
import ChartTableSwitcher from 'components/Replicator/Detail/ChartTableSwitcher';
import ThroughputTable from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs/ThroughputGraph/ThroughputTable';
import ThroughputTooltip from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs/ThroughputGraph/ThroughputTooltip';

const styles = (theme): StyleRules => {
  return {
    root: {
      position: 'relative',
    },
    heading: {
      marginBottom: '10px',
    },
    square: {
      display: 'inline-block',
      height: '15px',
      width: '15px',
      marginRight: '7px',
    },
    inserts: {
      backgroundColor: COLOR_MAP.inserts,
    },
    updates: {
      backgroundColor: COLOR_MAP.updates,
    },
    deletes: {
      backgroundColor: COLOR_MAP.deletes,
    },
    errors: {
      backgroundColor: COLOR_MAP.error,
      height: '15px',
      width: '15px',
      display: 'inline-block',
      borderRadius: '50%',
      color: theme.palette.white[50],
      textAlign: 'center',
      lineHeight: '15px',
      marginRight: '7px',
    },
    errorContainer: {
      marginLeft: 'auto',
    },
    bottomLegend: {
      marginTop: '25px',
      padding: '0 50px',
      display: 'flex',

      '& > div': {
        marginRight: '15px',
      },
    },
  };
};

interface IThroughputGraphProps extends WithStyles<typeof styles> {
  data: IThroughputLatencyData[];
}

const CONTAINER_ID = 'replication-throughput-graph';

const ThroughputGraphView: React.FC<IThroughputGraphProps> = ({ classes, data }) => {
  function renderTooltip(tooltip) {
    return <ThroughputTooltip tooltip={tooltip} />;
  }

  const chart = (
    <React.Fragment>
      <ChartContainer
        containerId={CONTAINER_ID}
        data={data}
        chartRenderer={renderThroughputGraph}
        watchWidth={true}
        renderTooltip={renderTooltip}
      />
      <div className={classes.bottomLegend}>
        <div>
          <div className={`${classes.square} ${classes.inserts}`} />
          <span>Inserts</span>
        </div>
        <div>
          <div className={`${classes.square} ${classes.updates}`} />
          <span>Updates</span>
        </div>
        <div>
          <div className={`${classes.square} ${classes.deletes}`} />
          <span>Deletes</span>
        </div>
        <div className={classes.errorContainer}>
          <div className={classes.errors}>!</div>
          <span>Errors</span>
        </div>
      </div>
    </React.Fragment>
  );

  const table = <ThroughputTable data={data} />;

  return (
    <div className={classes.root}>
      <Heading type={HeadingTypes.h4} label="Throughput" className={classes.heading} />
      <ChartTableSwitcher chart={chart} table={table} />
    </div>
  );
};

const ThroughputGraph = withStyles(styles)(ThroughputGraphView);
export default ThroughputGraph;
