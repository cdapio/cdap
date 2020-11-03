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
import { XYPlot, XAxis, YAxis, VerticalBarSeries, DiscreteColorLegend, Hint } from 'react-vis';
import { StyleRules, withStyles, WithStyles } from '@material-ui/core/styles';
import { ILatencyChildProps } from 'components/SystemServicesDelay/LatencyTypes';
import { orange, red } from 'components/ThemeWrapper/colors';
import Table from '@material-ui/core/Table';
import TableBody from '@material-ui/core/TableBody';
import TableRow from '@material-ui/core/TableRow';
import TableCell from '@material-ui/core/TableCell';

const CustomTableCell = withStyles((theme) => ({
  head: {
    backgroundColor: theme.palette.grey['300'],
    color: theme.palette.common.white,
    padding: 10,
    fontSize: 14,
  },
  body: {
    padding: 10,
    fontSize: 14,
  },
}))(TableCell);

const style = (theme): StyleRules => {
  return {
    chartContainer: {
      marginLeft: '20px',
      marginBottom: '10px',
    },
    row: {
      height: 40,
      '&:nth-of-type(odd)': {
        backgroundColor: theme.palette.grey['600'],
      },
    },
  };
};

interface ILatencyChartBaseProps extends WithStyles<typeof style>, ILatencyChildProps {}

const COLOR_LEGEND = [
  {
    title: 'Network Delay',
    color: orange[50],
  },
  {
    title: 'Backend Request Delay',
    color: red[50],
  },
];

function LatencyChartBase({ requests, classes }: ILatencyChartBaseProps) {
  const [popoverData, setPopoverData] = React.useState(null);
  const backendRequestDelayData = [];
  const networkDelayData = [];
  let counter = 1;
  for (const request of requests) {
    backendRequestDelayData.push({
      url: request.resource.url,
      id: request.id,
      x: counter,
      y: request.backendRequestTimeDuration,
    });
    networkDelayData.push({
      url: request.resource.url,
      id: request.id,
      x: counter,
      y: request.networkDelay || 0,
    });
    counter += 1;
  }
  if (!Array.isArray(backendRequestDelayData) || !backendRequestDelayData.length) {
    return null;
  }
  return (
    <div className={classes.chartContainer}>
      <XYPlot width={450} height={290} stackBy="y">
        <DiscreteColorLegend
          style={{ position: 'absolute', left: '5px', bottom: '5px', display: 'flex', gap: '10px' }}
          orientation="horizontal"
          items={COLOR_LEGEND}
        />
        <VerticalBarSeries
          onValueMouseOver={(d) => setPopoverData(d)}
          onValueMouseOut={() => setPopoverData(null)}
          cluster="latency"
          data={backendRequestDelayData}
          color={red[50]}
        />
        <VerticalBarSeries
          onValueMouseOver={(d) => setPopoverData(d)}
          onValueMouseOut={() => setPopoverData(null)}
          cluster="latency"
          data={networkDelayData}
          color={orange[50]}
        />
        <XAxis />
        <YAxis
          tickFormat={function tickFormat(d) {
            return d >= 1000
              ? d > 10000
                ? `${Math.ceil(d / 1000)}s`
                : `${(d / 1000).toFixed(1)}s`
              : d;
          }}
        />
        {popoverData && (
          <Hint
            value={popoverData}
            style={{
              background: 'white',
              borderRadius: '4px',
              boxShadow: '0px 10px 15px -5px rgba(0, 0, 0, 0.4)',
              maxWidh: '300px',
            }}
            align={{ vertical: 'top', horizontal: 'left' }}
          >
            <Table>
              <TableBody>
                <TableRow className={classes.row}>
                  <CustomTableCell>Path</CustomTableCell>
                  <CustomTableCell>{popoverData.url}</CustomTableCell>
                </TableRow>
                <TableRow className={classes.row}>
                  <CustomTableCell>Backend Latency</CustomTableCell>
                  <CustomTableCell>
                    {popoverData.y0 < 1000
                      ? `${popoverData.y0}ms`
                      : `${(popoverData.y0 / 1000).toFixed(1)}s`}
                  </CustomTableCell>
                </TableRow>
                <TableRow className={classes.row}>
                  <CustomTableCell>Network Latency</CustomTableCell>
                  <CustomTableCell>
                    {popoverData.y < 1000
                      ? `${popoverData.y}ms`
                      : `${(popoverData.y / 1000).toFixed(1)}s`}
                  </CustomTableCell>
                </TableRow>
              </TableBody>
            </Table>
          </Hint>
        )}
      </XYPlot>
    </div>
  );
}

export const LatencyChart = withStyles(style)(LatencyChartBase);
