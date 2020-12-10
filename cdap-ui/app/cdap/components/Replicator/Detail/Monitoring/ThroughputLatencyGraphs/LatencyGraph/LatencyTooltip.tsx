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
import { IThroughputLatencyData } from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs/parser';
import { formatLatency } from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs/LatencyGraph/LatencyTable';

export const tooltipWidth = 250;
const styles = (theme): StyleRules => {
  return {
    tooltip: {
      position: 'absolute',
      backgroundColor: theme.palette.white[50],
      boxShadow: theme.shadows[3],
      padding: '15px',
      width: `${tooltipWidth}px`,
    },
    heading: {
      marginBottom: '10px',
    },
  };
};

interface ILatencyTooltipProps extends WithStyles<typeof styles> {
  tooltip: {
    top: number;
    left: number;
    isOpen: boolean;
    activeData?: Partial<IThroughputLatencyData>;
  };
}

const LatencyTooltipView: React.FC<ILatencyTooltipProps> = ({ classes, tooltip }) => {
  const tooltipData: Partial<IThroughputLatencyData> =
    tooltip && tooltip.activeData ? tooltip.activeData : {};
  return (
    <div
      className={classes.tooltip}
      style={{
        top: tooltip.top,
        left: tooltip.left,
        opacity: tooltip.isOpen ? 1 : 0,
      }}
    >
      <div className={classes.heading}>
        <strong>{tooltipData.formattedTimeRange}</strong>
      </div>
      <div>
        <div>Average latency</div>
        <div>{formatLatency(tooltipData.latency || 0)}</div>
      </div>
    </div>
  );
};

const LatencyTooltip = withStyles(styles)(LatencyTooltipView);
export default LatencyTooltip;
