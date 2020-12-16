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
import { COLOR_MAP } from 'components/Replicator/Detail/Monitoring/ThroughputLatencyGraphs/ThroughputGraph/throughput';
import { convertBytesToHumanReadable } from 'services/helpers';

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
    square: {
      height: '15px',
      width: '15px',
      display: 'inline-block',
    },
    heading: {
      marginBottom: '10px',
    },
    metricContent: {
      marginBottom: '5px',

      '& > *': {
        marginRight: '10px',
      },
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
    },
  };
};

export interface ITooltipInfo {
  top: number;
  left: number;
  isOpen: boolean;
  activeData?: Partial<IThroughputLatencyData>;
}

interface IThroughputTooltipProps extends WithStyles<typeof styles> {
  tooltip: ITooltipInfo;
}

const ThroughputTooltipView: React.FC<IThroughputTooltipProps> = ({ classes, tooltip }) => {
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
      <div className={classes.metricContent}>
        <div className={classes.square} />
        <span>Data replicated:</span>
        <span>{convertBytesToHumanReadable(tooltipData.dataReplicated, null, true)}</span>
      </div>
      <div>
        <div className={classes.metricContent}>
          <div className={`${classes.square} ${classes.deletes}`} />
          <span>Deletes:</span>
          <span>{tooltipData.deletes}</span>
        </div>

        <div className={classes.metricContent}>
          <div className={`${classes.square} ${classes.updates}`} />
          <span>Updates:</span>
          <span>{tooltipData.updates}</span>
        </div>

        <div className={classes.metricContent}>
          <div className={`${classes.square} ${classes.inserts}`} />
          <span>Inserts:</span>
          <span>{tooltipData.inserts}</span>
        </div>

        <div className={classes.metricContent}>
          <div className={`${classes.square} ${classes.errors}`} />
          <span>Errors:</span>
          <span>{tooltipData.errors}</span>
        </div>
      </div>
    </div>
  );
};

const ThroughputTooltip = withStyles(styles)(ThroughputTooltipView);
export default ThroughputTooltip;
