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
import {
  XYPlot,
  AreaSeries,
  makeVisFlexible,
  XAxis,
  YAxis,
  HorizontalGridLines,
  LineMarkSeries,
  Hint,
} from 'react-vis';
import NodeMetricsGraphLegends from 'components/PipelineNodeGraphs/NodeMetricsGraphLegends';
import { getYAxisProps } from 'components/PipelineSummary/RunsGraphHelpers';
import { humanReadableDate, objectQuery } from 'services/helpers';
import maxBy from 'lodash/maxBy';
import findKey from 'lodash/findKey';
import findIndex from 'lodash/findIndex';
import capitalize from 'lodash/capitalize';
import NodeMetricsSingleDatapoint from 'components/PipelineNodeGraphs/NodeMetricsSingleDatapoint';
import isEqual from 'lodash/isEqual';
import T from 'i18n-react';

const RECORDS_IN_COLOR = '#58B7F6';
const RECORDS_OUT_COLOR = '#97A0BA';
const PREFIX = `features.PipelineSummary.pipelineNodesMetricsGraph.NodeMetricsGraph`;
const metricTypeToColorMap = {
  recordsin: RECORDS_IN_COLOR,
  recordsout: RECORDS_OUT_COLOR,
};

const metricLabelMap = {
  recordsIn: {
    label: T.translate(`${PREFIX}.recordsIn`),
    numOfRecordsLabel: T.translate(`${PREFIX}.numOfRecordsIn`),
  },
  recordsOut: {
    label: T.translate(`${PREFIX}.recordsOut`),
    numOfRecordsLabel: T.translate(`${PREFIX}.numOfRecordsOut`),
  },
  recordsError: {
    label: T.translate(`${PREFIX}.recordsError`),
    numOfRecordsLabel: T.translate(`${PREFIX}.numOfRecordsError`),
  },
};

function isDataSeriesHaveSingleDatapoint(data) {
  if (!data) {
    return false;
  }
  if (Array.isArray(data) && data.length === 1) {
    return true;
  }
  if (typeof data === 'object') {
    let allObjectsHaveSingleDataPoint = true;
    Object.keys(data).forEach((d) => {
      if (data[d].data.length > 1) {
        allObjectsHaveSingleDataPoint = false;
      }
    });
    return allObjectsHaveSingleDataPoint;
  }
}

export { isDataSeriesHaveSingleDatapoint };

export default class NodeMetricsGraph extends Component {
  static propTypes = {
    xAxisTitle: PropTypes.string,
    yAxisTitle: PropTypes.string,
    data: PropTypes.arrayOf(PropTypes.object),
    metricType: PropTypes.string,
    isMultiplePorts: PropTypes.bool,
    portsToShow: PropTypes.arrayOf(PropTypes.string),
  };

  constructor(props) {
    super(props);

    this.data = this.props.data;

    let dataToShow = Object.keys(this.data).map((dataKey) => this.data[dataKey].label);

    if (this.props.isMultiplePorts && this.props.portsToShow && this.props.portsToShow.length) {
      let portsToShow = this.props.portsToShow;
      // always show error records no matter what port the user clicks on
      if (portsToShow.indexOf('errors') === -1) {
        portsToShow.push('errors');
      }
      dataToShow = portsToShow.map((port) => capitalize(port));
    }

    this.state = {
      dataToShow,
      currentHoveredElement: null,
    };

    if (Array.isArray(this.data) && !this.data.length) {
      return;
    }

    this.xDomain = [0, 10];
    let { tickFormat, yDomain } = getYAxisProps(this.data);
    this.tickFormat = tickFormat;
    this.yDomain = yDomain;
    this.dummyData = [
      {
        x: 0,
        y: 0,
      },
    ];

    this.colorLegend = [];

    if (Array.isArray(this.data) && this.data.length > 10) {
      this.xDomain[1] = this.data.length;
      this.colorLegend = [
        {
          title: this.data.label,
          color: this.data.color,
        },
      ];
    } else if (!Array.isArray(this.data) && typeof this.data === 'object') {
      let dataObjects = Object.keys(this.data).map((key) => this.data[key]);
      let objectWithMaxLength = maxBy(dataObjects, (dataObject) => dataObject.data.length);
      this.xDomain[1] = objectWithMaxLength.data.length;
      Object.keys(this.data).forEach((d) => {
        this.colorLegend.push({
          title: this.data[d].label,
          color: this.data[d].color,
        });
      });
    }
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
      dataToShow: newDataToShow,
    });
  };

  renderAreaChart(data, metricType) {
    if (!Array.isArray(data) && typeof data === 'object') {
      let dataArrayToShow = Object.keys(data).filter(
        (key) => this.state.dataToShow.indexOf(data[key].label) !== -1
      );
      if (!dataArrayToShow.length) {
        return (
          <AreaSeries
            curve="curveLinear"
            color={metricTypeToColorMap[metricType]}
            stroke={metricTypeToColorMap[metricType]}
            data={this.dummyData}
            opacity={0.2}
          />
        );
      }

      return dataArrayToShow.map((key) => {
        let dataToPlot = data[key].data;
        if (!dataToPlot || !dataToPlot.length) {
          return null;
        }
        return (
          <AreaSeries
            curve="curveLinear"
            color={data[key].color || metricTypeToColorMap[metricType]}
            stroke={data[key].color || metricTypeToColorMap[metricType]}
            data={dataToPlot}
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

  renderLineChart = (data, metricType) => {
    const onValueMouseOver = (key, d) => {
      this.state.currentHoveredElement ? delete this.state.currentHoveredElement['key'] : null;
      if (isEqual(this.state.currentHoveredElement, d)) {
        return;
      }
      this.setState({
        currentHoveredElement: { ...d, key },
      });
    };
    if (!Array.isArray(data) && typeof data === 'object') {
      return Object.keys(data)
        .filter((key) => this.state.dataToShow.indexOf(data[key].label) !== -1)
        .map((key) => {
          if (Array.isArray(data[key].data) && !data[key].data.length) {
            return null;
          }
          return (
            <LineMarkSeries
              color={data[key].color || metricTypeToColorMap[metricType]}
              data={data[key].data}
              strokeWidth={4}
              size={3}
              onValueMouseOut={() => {
                this.setState({
                  currentHoveredElement: null,
                });
              }}
              onValueMouseOver={onValueMouseOver.bind(this, key)}
            />
          );
        });
    }
    return (
      <LineMarkSeries
        color={metricTypeToColorMap[metricType]}
        data={data}
        strokeWidth={4}
        size={3}
        onValueMouseOut={() => {
          this.setState({
            currentHoveredElement: null,
          });
        }}
        onValueMouseOver={onValueMouseOver.bind(this)}
      />
    );
  };
  render() {
    if (Array.isArray(this.props.data) && !this.props.data.length) {
      return null;
    }
    let FPlot = makeVisFlexible(XYPlot);
    if (isDataSeriesHaveSingleDatapoint(this.props.data)) {
      return (
        <div className="graph-plot-container single-datapoint-graph">
          <NodeMetricsSingleDatapoint data={this.props.data} />
        </div>
      );
    }
    let metricLabel =
      objectQuery(this.state, 'currentHoveredElement', 'key') || this.props.metricType;
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
          {this.state.currentHoveredElement ? (
            <Hint value={this.state.currentHoveredElement}>
              <div className="title">
                <h4>
                  {metricLabelMap[metricLabel]
                    ? metricLabelMap[metricLabel].label
                    : `${metricLabelMap['recordsOut'].label} (${metricLabel})`}
                </h4>
              </div>
              <div className="log-stats">
                <div>
                  <span>
                    {metricLabelMap[metricLabel]
                      ? metricLabelMap[metricLabel].numOfRecordsLabel
                      : metricLabelMap['recordsOut'].numOfRecordsLabel}
                  </span>
                  <span>{this.state.currentHoveredElement.actualRecords || '0'}</span>
                </div>
                <div>
                  <span>{T.translate(`${PREFIX}.ts`)}</span>
                  <span>{humanReadableDate(this.state.currentHoveredElement.time, true)}</span>
                </div>
                <div>
                  <span>{T.translate(`${PREFIX}.accumulatedRecords`)}</span>
                  <span>{this.state.currentHoveredElement.y}</span>
                </div>
              </div>
            </Hint>
          ) : null}
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
