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
import { convertBytesToHumanReadable, truncateNumber } from 'services/helpers';
import { formatNumber, getTableDisplayName } from 'components/Replicator/utilities';

const styles = (): StyleRules => {
  return {
    grid: {
      '&.grid-wrapper': {
        height: '100%',

        '& .grid.grid-container.grid-compact': {
          maxHeight: '415px',

          '& .grid-header > .grid-row': {
            alignItems: 'end',
          },

          '& .grid-row': {
            gridTemplateColumns: '3fr 2fr 2fr 2fr 2fr 2fr',

            '& > div:not(:first-child)': {
              textAlign: 'right',
            },
          },
        },
      },
    },
  };
};

interface IScatterPlotTableProps extends WithStyles<typeof styles> {
  data: ITableMetricsData[];
}

const ScatterPlotTableView: React.FC<IScatterPlotTableProps> = ({ classes, data }) => {
  return (
    <div className={`grid-wrapper ${classes.grid}`}>
      <div className="grid grid-container grid-compact">
        <div className="grid-header">
          <div className="grid-row">
            <div>Table name</div>
            <div>Data replicated</div>
            <div>
              <div>Throughput</div>
              <div>(events/min)</div>
            </div>
            <div>
              <div>Avg. latency</div>
              <div>(min)</div>
            </div>
            <div>
              <div>Events</div>
              <div>processed</div>
            </div>
            <div>Errors</div>
          </div>
        </div>

        <div className="grid-body">
          {data.map((table) => {
            const PRECISION = 2;
            const eventsPerMin = truncateNumber(table.eventsPerMin, PRECISION);
            const latency = truncateNumber(table.latency, PRECISION);
            const dataReplicated = convertBytesToHumanReadable(table.dataReplicated, null, true);
            const tableDisplayName = getTableDisplayName(table.tableInfo);

            return (
              <div className="grid-row" key={tableDisplayName}>
                <div>{tableDisplayName}</div>
                <div>{dataReplicated}</div>
                <div>{eventsPerMin}</div>
                <div>{latency}</div>
                <div>{formatNumber(table.totalEvents)}</div>
                <div>{table.errors}</div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
};

const ScatterPlotTable = withStyles(styles)(ScatterPlotTableView);
export default ScatterPlotTable;
