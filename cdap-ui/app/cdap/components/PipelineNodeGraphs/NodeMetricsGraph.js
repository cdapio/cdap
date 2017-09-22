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

import PropTypes from 'prop-types';

import React from 'react';
import {XYPlot, AreaSeries, makeVisFlexible, XAxis, YAxis, HorizontalGridLines, LineSeries, DiscreteColorLegend} from 'react-vis';
import {getYAxisProps} from 'components/PipelineSummary/RunsGraphHelpers';

const RECORDS_IN_COLOR = '#58B7F6';
const RECORDS_OUT_COLOR = '#97A0BA';
const metricTypeToColorMap = {
  'recordsin': RECORDS_IN_COLOR,
  'recordsout': RECORDS_OUT_COLOR
};

function renderAreaChart(data, metricType) {
  if (!Array.isArray(data) && typeof data === 'object') {
    return Object
      .keys(data)
      .map(key => {
        return (
          <AreaSeries
            curve="curveLinear"
            color={data[key].color || metricTypeToColorMap[metricType]}
            stroke={data[key].color || metricTypeToColorMap[metricType]}
            data={data[key].data}
            opacity={0.2}
          />
        );
      });
  }
  return (
    <AreaSeries
      curve="curveLinear"
      color={metricTypeToColorMap[metricType]}
      stroke={metricTypeToColorMap[metricType]}
      data={data}
      opacity={0.5}
    />
  );
}

function renderLineChart(data, metricType) {
  if (!Array.isArray(data) && typeof data === 'object') {
    return Object
      .keys(data)
      .map(key => {
        return (
          <LineSeries
            color={data[key].color || metricTypeToColorMap[metricType]}
            data={data[key].data}
            strokeWidth={4}
          />
        );
      });
  }
  return (
    <LineSeries
      color={metricTypeToColorMap[metricType]}
      data={data}
      strokeWidth={4}
    />
  );
}

export default function NodeMetricsGraph({data, xAxisTitle, yAxisTitle, metricType}) {
  if (Array.isArray(data) && !data.length) {
    return null;
  }
  let xDomain = [0, 10];
  let {tickFormat, yDomain} = getYAxisProps(data);
  var color_legend = [];
  if (Array.isArray(data) && data.length > 10) {
    xDomain[1] = data.length;
    color_legend = [{
      title: data.label,
      color: data.color
    }];
  } else if (!Array.isArray(data) && typeof data === 'object') {
    let firstKey = Object.keys(data)[0];
    xDomain[1] = data[firstKey].data.length;
    Object
      .keys(data)
      .forEach(d => {
        color_legend.push({
          title: data[d].label,
          color: data[d].color
        });
      });
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
        {renderAreaChart(data, metricType)}
        {renderLineChart(data, metricType)}
        <div className="x-axis-title">{xAxisTitle}</div>
        <div className="y-axis-title">{yAxisTitle}</div>
      </FPlot>
      <DiscreteColorLegend
        style={{position: 'absolute', left: '40px', top: '0px'}}
        orientation="horizontal"
        items={color_legend}
      />
    </div>
  );
}

NodeMetricsGraph.propTypes = {
  xAxisTitle: PropTypes.string,
  yAxisTitle: PropTypes.string,
  data: PropTypes.arrayOf(PropTypes.object),
  metricType: PropTypes.string
};

