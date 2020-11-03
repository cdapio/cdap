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
import {
  XYPlot,
  makeVisFlexible,
  XAxis,
  YAxis,
  HorizontalGridLines,
  VerticalBarSeries,
  MarkSeries,
  DiscreteColorLegend,
  Hint,
} from 'react-vis';

import { StyleRules, withStyles, WithStyles } from '@material-ui/core/styles';
import { ILatencyChildProps } from 'components/SystemServicesDelay/LatencyTypes';
import { orange, red } from 'components/ThemeWrapper/colors';

const style = (): StyleRules => {
  return {
    chartContainer: {
      marginLeft: '20px',
      marginBottom: '10px',
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
    title: 'Request Delay',
    color: red[50],
  },
];

function LatencyChartBase({ requestDelayMap, classes }: ILatencyChartBaseProps) {
  const requestDelayData = [];
  const networkDelayData = [];
  let counter = 1;
  for (const [id, value] of Object.entries(requestDelayMap)) {
    requestDelayData.push({
      id,
      x: counter,
      y: value.backendRequestTimeDuration,
    });
    networkDelayData.push({
      id,
      x: counter,
      y: value.requestTimeFromClient || 0,
    });
    counter += 1;
  }
  if (!Array.isArray(requestDelayData) || !requestDelayData.length) {
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
        <VerticalBarSeries cluster="latency" data={requestDelayData} color={red[50]} />
        <VerticalBarSeries cluster="latency" data={networkDelayData} color={orange[50]} />
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
      </XYPlot>
    </div>
  );
}

export const LatencyChart = withStyles(style)(LatencyChartBase);
