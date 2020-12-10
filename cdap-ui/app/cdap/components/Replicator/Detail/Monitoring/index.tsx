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
import Metrics from 'components/Replicator/Detail/Monitoring/Metrics';
import TimePeriodSelection from 'components/Replicator/Detail/Monitoring/TimePeriodSelection';
import ThroughputLatencyGraphs from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs';
import TableScatterPlotGraph from 'components/Replicator/Detail/Monitoring/TableScatterPlotGraph';
import SelectedTable from 'components/Replicator/Detail/Monitoring/SelectedTable';

const styles = (theme): StyleRules => {
  return {
    root: {
      marginTop: '25px',

      '& > hr': {
        borderWidth: '2px',
        borderColor: theme.palette.grey[200],
      },
    },
  };
};

const MonitoringView: React.FC<WithStyles<typeof styles>> = ({ classes }) => {
  return (
    <div className={classes.root}>
      <TimePeriodSelection />
      <Metrics />
      <hr />
      <TableScatterPlotGraph />
      <hr />
      <SelectedTable />
      <ThroughputLatencyGraphs />
    </div>
  );
};

const Monitoring = withStyles(styles)(MonitoringView);
export default Monitoring;
