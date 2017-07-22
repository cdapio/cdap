/*
 * Copyright © 2017 Cask Data, Inc.
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

import React, {PropTypes} from 'react';
import {XYPlot, AreaSeries, makeVisFlexible, XAxis, YAxis, HorizontalGridLines, LineSeries} from 'react-vis';
import {getYAxisProps} from 'components/PipelineSummary/RunsGraphHelpers';

const RECORDS_IN_COLOR = '#58B7F6';
const RECORDS_OUT_COLOR = '#97A0BA';
const metricTypeToColorMap = {
  'recordsin': RECORDS_IN_COLOR,
  'recordsout': RECORDS_OUT_COLOR
};

export default function NodeMetricsGraph({data, xAxisTitle, yAxisTitle, metricType}) {
  if (!data.length) {
    return null;
  }
  let xDomain = [0, 10];
  let {tickFormat, yDomain} = getYAxisProps(data);
  if (data.length > 10) {
    xDomain[1] = data.length;
  }
  let FPlot = makeVisFlexible(XYPlot);
  return (
    <div className="graph-plot-container">
      <FPlot
        xType="linear"
        yType="linear"
        xDomain={xDomain}
        yDomain={yDomain}
        tickTotal={xDomain[1]}
        className="run-history-fp-plot"
      >
        <XAxis />
        <YAxis tickFormat={tickFormat} />
        <HorizontalGridLines />
        <AreaSeries
          curve="curveLinear"
          color={metricTypeToColorMap[metricType]}
          stroke={metricTypeToColorMap[metricType]}
          data={data}
          opacity={0.5}
        />
        <LineSeries
          color={metricTypeToColorMap[metricType]}
          data={data}
          strokeWidth={4}
        />
        <div className="x-axis-title">{xAxisTitle}</div>
        <div className="y-axis-title">{yAxisTitle}</div>
      </FPlot>
    </div>
  );
}

NodeMetricsGraph.propTypes = {
  xAxisTitle: PropTypes.string,
  yAxisTitle: PropTypes.string,
  data: PropTypes.arrayOf(PropTypes.object),
  metricType: PropTypes.string
};

