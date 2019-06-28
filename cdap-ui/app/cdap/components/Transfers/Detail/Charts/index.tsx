/*
 * Copyright © 2019 Cask Data, Inc.
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
import { transfersDetailConnect } from 'components/Transfers/Detail/context';
import Gauge from './Gauge';
import Latency from './Latency';
import Events from './Events';

const styles = (theme): StyleRules => {
  return {
    root: {
      display: 'grid',
      gridTemplateColumns: '20% 1fr 1fr',
      alignItems: 'center',
      gridColumnGap: '50px',
      height: '300px',
      margin: '0 25px',
      '& > div': {
        height: '300px',
      },
    },
  };
};

interface ICharts extends WithStyles<typeof styles> {}

const ChartsView: React.SFC<ICharts> = ({ classes }) => {
  return (
    <div className={classes.root}>
      <Gauge />
      <Latency />
      <Events />
      <div />
    </div>
  );
};

const StyledCharts = withStyles(styles)(ChartsView);
const Charts = transfersDetailConnect(StyledCharts);
export default Charts;
