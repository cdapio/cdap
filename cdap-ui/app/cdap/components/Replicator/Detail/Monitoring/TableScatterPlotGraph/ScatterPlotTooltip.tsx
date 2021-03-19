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
import { ITableMetricsData } from 'components/Replicator/Detail/Monitoring/TableScatterPlotGraph/parser';
import { convertBytesToHumanReadable, objectQuery, truncateNumber } from 'services/helpers';
import { getTableDisplayName } from 'components/Replicator/utilities';

export const tooltipWidth = 350;
const styles = (theme): StyleRules => {
  return {
    tooltip: {
      position: 'absolute',
      backgroundColor: theme.palette.white[50],
      boxShadow: theme.shadows[3],
      padding: '15px 0',
      width: `${tooltipWidth}px`,
    },
    tooltipHeading: {
      marginBottom: '15px',
      padding: '0 15px',
    },
    tooltipMetrics: {
      display: 'grid',
      gridTemplateColumns: '50% 50%',
    },
    metricContent: {
      padding: '0 15px',

      '&:first-child': {
        borderRight: `1px solid ${theme.palette.grey[300]}`,
      },

      '& > div': {
        display: 'grid',
        gridTemplateColumns: '50% 50%',
      },
    },
    metric: {
      textAlign: 'right',
    },
    dataReplicated: {
      display: 'flex',
      width: '250px',
      marginLeft: '15px',
    },
    dataMetric: {
      marginLeft: '10px',
    },
  };
};

const PRECISION = 2;

interface IScatterPlotTooltipProps extends WithStyles<typeof styles> {
  tooltip: {
    top: number;
    left: number;
    isOpen: boolean;
    activeData?: Partial<ITableMetricsData>;
  };
}

const ScatterPlotTooltipView: React.FC<IScatterPlotTooltipProps> = ({ classes, tooltip }) => {
  const activeTooltip: Partial<ITableMetricsData> =
    tooltip && tooltip.activeData ? tooltip.activeData : {};

  const tableInfo = objectQuery(tooltip, 'activeData', 'tableInfo') || {};
  const displayTableName = getTableDisplayName(tableInfo);

  return (
    <div
      className={classes.tooltip}
      style={{
        top: tooltip.top,
        left: tooltip.left,
        opacity: tooltip.isOpen ? 1 : 0,
      }}
    >
      <div className={classes.tooltipHeading}>
        <strong>{displayTableName}</strong>
      </div>
      <div className={classes.dataReplicated}>
        <strong>Data replicated</strong>
        <span className={classes.dataMetric}>
          {convertBytesToHumanReadable(activeTooltip.dataReplicated, null, true)}
        </span>
      </div>
      <div className={classes.tooltipMetrics}>
        <div className={classes.metricContent}>
          <div>
            <strong>Throughput</strong>
            <div className={classes.metric}>
              {truncateNumber(activeTooltip.eventsPerMin, PRECISION)}
            </div>
          </div>
          <div>
            <strong>Latency</strong>
            <div className={classes.metric}>{truncateNumber(activeTooltip.latency, PRECISION)}</div>
          </div>
        </div>

        <div className={classes.metricContent}>
          <div>
            <strong>Events</strong>
            <div className={classes.metric}>{activeTooltip.totalEvents}</div>
          </div>
          <div>
            <strong>Errors</strong>
            <div className={classes.metric}>{activeTooltip.errors}</div>
          </div>
        </div>
      </div>
    </div>
  );
};

const ScatterPlotTooltip = withStyles(styles)(ScatterPlotTooltipView);
export default ScatterPlotTooltip;
