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
  LineSeries,
  MarkSeries,
  Hint,
} from 'react-vis';
import {
  getXDomain,
  getTicksTotal,
  getYAxisProps,
  xTickFormat,
} from 'components/PipelineSummary/RunsGraphHelpers';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import moment from 'moment';
import ee from 'event-emitter';
import { preventPropagation, objectQuery } from 'services/helpers';
import colorVariables from 'styles/variables.scss';

const PREFIX = `features.PipelineSummary.nodesMetricsGraph`;
const RECORDS_IN_COLOR = colorVariables.blue04;
// TODO: CDAP-17725
const RECORDS_OUT_COLOR = '#97A0BA';
const HOVER_COLOR = 'black';
const MARK_SERIES_FILL_COLOR = 'white';
const MARK_SERIES_STROKE_COLOR = 'gray';

const getAreaColor = (recordType = 'recordsin') => {
  return recordType === 'recordsin' ? RECORDS_IN_COLOR : RECORDS_OUT_COLOR;
};

export default class NodesRecordsGraph extends Component {
  constructor(props) {
    super(props);
    this.state = {
      data: [],
      currentHoveredElement: null,
    };
    this.eventEmitter = ee(ee);
    this.closeTooltip = this.closeTooltip.bind(this);
  }
  componentDidMount() {
    this.setState({
      data: this.constructData(),
    });
    this.eventEmitter.on('CLOSE_HINT_TOOLTIP', this.closeTooltip);
  }
  closeTooltip() {
    this.setState({
      currentHoveredElement: null,
    });
  }
  componentWillReceiveProps(nextProps) {
    this.setState({
      data: this.constructData(nextProps),
      totalRunsCount: nextProps.totalRunsCount,
      currentHoveredElement: null,
      runsLimit: nextProps.runsLimit,
    });
  }
  componentWillUnmount() {
    this.eventEmitter.off('CLOSE_HINT_TOOLTIP', this.closeTooltip);
  }
  constructData(props = this.props) {
    let data = []
      .concat(props.records)
      .reverse()
      .map((run, id) => {
        let { totalRunsCount, runsLimit, xDomainType } = this.props;
        let x;
        if (xDomainType === 'limit') {
          x = totalRunsCount > runsLimit ? totalRunsCount - runsLimit : 0;
          x = x + (id + 1);
        }
        if (xDomainType === 'time') {
          x = run.starting;
        }
        return {
          x,
          y: run.numberOfRecords,
          color: 'gray',
          runid: run.runid,
        };
      });
    return data;
  }
  renderChart() {
    let FPlot = makeVisFlexible(XYPlot);
    let { yDomain, tickFormat } = getYAxisProps(this.state.data);
    let popOverData;
    if (this.state.currentHoveredElement) {
      popOverData = this.props.records.find(
        (run) => this.state.currentHoveredElement.runid === run.runid
      );
    }
    let xDomain = [];
    if (this.state.data.length > 0) {
      xDomain = getXDomain(this.props);
    }
    return (
      <div className="graph-plot-container">
        <FPlot
          xType="linear"
          yType="linear"
          xDomain={xDomain}
          yDomain={yDomain}
          className="run-history-fp-plot"
        >
          <XAxis tickTotal={getTicksTotal(this.props)} tickFormat={xTickFormat(this.props)} />
          <YAxis tickFormat={tickFormat} />
          <HorizontalGridLines />
          <AreaSeries
            curve="curveLinear"
            color={getAreaColor(this.props.recordType)}
            fill={getAreaColor(this.props.recordType)}
            stroke={getAreaColor(this.props.recordType)}
            data={this.state.data.map((datum) => ({ x: datum.x, y: datum.y, runid: datum.runid }))}
            opacity={0.2}
          />
          <LineSeries
            color={getAreaColor(this.props.recordType)}
            stroke={getAreaColor(this.props.recordType)}
            data={this.state.data.map((datum) => ({ x: datum.x, y: datum.y, runid: datum.runid }))}
            strokeWidth={4}
          />
          <MarkSeries
            data={this.state.data}
            strokeType="literal"
            fillType="literal"
            fill={MARK_SERIES_FILL_COLOR}
            stroke={MARK_SERIES_STROKE_COLOR}
            strokeWidth={1}
            onValueMouseOver={(d) => {
              if (objectQuery(this.state.currentHoveredElement, 'runid') === d.runid) {
                return;
              }
              const newData = this.state.data.map((datum) => {
                if (datum.runid === d.runid) {
                  return {
                    ...datum,
                    fill: HOVER_COLOR,
                  };
                }
                return datum;
              });
              this.setState({
                currentHoveredElement: d,
                data: newData,
              });
            }}
            onValueMouseOut={() => {
              const newData = this.state.data.map((datum) => {
                return {
                  ...datum,
                  fill: MARK_SERIES_FILL_COLOR,
                };
              });
              this.setState({
                currentHoveredElement: null,
                data: newData,
              });
            }}
          />

          {this.state.currentHoveredElement && popOverData ? (
            <Hint value={this.state.currentHoveredElement}>
              <div className="title">
                <span>{this.props.activeNode}</span>
                <IconSVG
                  name="icon-close"
                  onClick={(e) => {
                    const newData = this.state.data.map((datum) => {
                      return {
                        ...datum,
                        fill: MARK_SERIES_FILL_COLOR,
                      };
                    });
                    this.setState({
                      currentHoveredElement: null,
                      data: newData,
                    });
                    preventPropagation(e);
                  }}
                />
              </div>
              <strong>
                {T.translate(`${PREFIX}.${this.props.recordType}.hint.title`, {
                  count: popOverData.numberOfRecords || '0',
                })}{' '}
              </strong>
              {this.props.xDomainType === 'limit' ? (
                <div>
                  <strong>{T.translate(`${PREFIX}.hint.runNumber`)}: </strong>
                  <span>{this.state.currentHoveredElement.x}</span>
                </div>
              ) : null}
              <div>
                <strong>{T.translate(`${PREFIX}.hint.startTime`)}: </strong>
                <span>{moment(popOverData.starting * 1000).format('llll')}</span>
              </div>
            </Hint>
          ) : null}
          {this.props.xDomainType === 'limit' ? (
            <div className="x-axis-title"> {T.translate(`${PREFIX}.xAxisTitle`)} </div>
          ) : null}
          <div className="y-axis-title">{T.translate(`${PREFIX}.yAxisTitle`)}</div>
        </FPlot>
      </div>
    );
  }
  render() {
    return this.renderChart();
  }
}
NodesRecordsGraph.propTypes = {
  records: PropTypes.arrayOf(PropTypes.object),
  totalRunsCount: PropTypes.number,
  runsLimit: PropTypes.number,
  xDomainType: PropTypes.oneOf(['limit', 'time']),
  runContext: PropTypes.object,
  isLoading: PropTypes.bool,
  start: PropTypes.number,
  end: PropTypes.number,
  activeFilterLabel: PropTypes.string,
  activeNode: PropTypes.string,
  recordType: PropTypes.oneOf(['recordsin', 'recordsout']),
};
