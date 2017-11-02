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

import React, { Component } from 'react';
import {XYPlot, AreaSeries, makeVisFlexible, XAxis, YAxis, HorizontalGridLines, LineSeries} from 'react-vis';
import NodeMetricsGraphLegends from 'components/PipelineNodeGraphs/NodeMetricsGraphLegends';
import {getYAxisProps} from 'components/PipelineSummary/RunsGraphHelpers';
import maxBy from 'lodash/maxBy';
import findKey from 'lodash/findKey';
import findIndex from 'lodash/findIndex';

const RECORDS_IN_COLOR = '#58B7F6';
const RECORDS_OUT_COLOR = '#97A0BA';
const metricTypeToColorMap = {
  'recordsin': RECORDS_IN_COLOR,
  'recordsout': RECORDS_OUT_COLOR
};

const FPlot = makeVisFlexible(XYPlot);

export default class NodeMetricsGraph extends Component {

  static propTypes = {
    xAxisTitle: PropTypes.string,
    yAxisTitle: PropTypes.string,
    data: PropTypes.arrayOf(PropTypes.object),
    metricType: PropTypes.string,
    isMultiplePorts: PropTypes.bool
  };

  constructor(props) {
    super(props);

    this.data = this.props.data;

    let dataToShow = Object.keys(this.data).map(dataKey => this.data[dataKey].label);

    this.state = {
      dataToShow
    };

    if (Array.isArray(this.data) && !this.data.length) {
      return;
    }

    this.xDomain = [0, 10];
    let {tickFormat, yDomain} = getYAxisProps(this.data);
    this.tickFormat = tickFormat;
    this.yDomain = yDomain;

    this.colorLegend = [];

    if (Array.isArray(this.data) && this.data.length > 10) {
      this.xDomain[1] = this.data.length;
      this.colorLegend = [{
        title: this.data.label,
        color: this.data.color
      }];
    } else if (!Array.isArray(this.data) && typeof this.data === 'object') {
      let dataObjects = Object.keys(this.data).map((key) => this.data[key]);
      let objectWithMaxLength = maxBy(dataObjects, (dataObject) => dataObject.data.length);
      this.xDomain[1] = objectWithMaxLength.data.length;
      Object
        .keys(this.data)
        .forEach(d => {
          this.colorLegend.push({
            title: this.data[d].label,
            color: this.data[d].color
          });
        });
    }

    this.onLegendClick = this.onLegendClick.bind(this);
  }

  onLegendClick = (itemTitle) => {
    let legendKey = findKey(this.data, (dataKey) => dataKey.label === itemTitle);
    if (!legendKey) {
      return;
    }

    let legendItem = this.data[legendKey];
    let newDataToShow = [...this.state.dataToShow];
    let legendItemIndex = findIndex(newDataToShow, (dataKey) => dataKey === legendItem.label);
    if (legendItemIndex !== -1) {
      newDataToShow.splice(legendItemIndex, 1);
    } else {
      newDataToShow.push(legendItem.label);
    }
    this.setState({
      dataToShow: newDataToShow
    });
  };

  renderAreaChart(data, metricType) {
    if (!Array.isArray(data) && typeof data === 'object') {
      return Object
        .keys(data)
        .filter(key => this.state.dataToShow.indexOf(data[key].label) !== -1)
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

  renderLineChart(data, metricType) {
    if (!Array.isArray(data) && typeof data === 'object') {
      return Object
        .keys(data)
        .filter(key => this.state.dataToShow.indexOf(data[key].label) !== -1)
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

  render() {
    return (
      <div className="graph-plot-container">
        <FPlot
          xType="linear"
          yType="linear"
          xDomain={this.xDomain}
          yDomain={this.yDomain}
          tickTotal={this.xDomain[1]}
          className="run-history-fp-plot"
        >
          <XAxis />
          <YAxis tickFormat={this.tickFormat} />
          <HorizontalGridLines />
          {this.renderAreaChart(this.data, this.metricType)}
          {this.renderLineChart(this.data, this.metricType)}
          <div className="x-axis-title">{this.props.xAxisTitle}</div>
          <div className="y-axis-title">{this.props.yAxisTitle}</div>
        </FPlot>
        <NodeMetricsGraphLegends
          items={this.colorLegend}
          checkedItems={this.state.dataToShow}
          onLegendClick={this.onLegendClick}
          isMultiplePorts={this.props.isMultiplePorts}
        />
      </div>
    );
  }
}

