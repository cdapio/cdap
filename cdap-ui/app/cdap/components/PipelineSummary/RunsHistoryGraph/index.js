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

import React, {Component, PropTypes} from 'react';
import {XYPlot, makeWidthFlexible, XAxis, YAxis, HorizontalGridLines, LineSeries, MarkSeries, DiscreteColorLegend, Hint} from 'react-vis';
import moment from 'moment';
import classnames from 'classnames';
import isEqual from 'lodash/isEqual';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import {
  getTicksTotal,
  xTickFormat,
  getXDomain,
  ONE_MIN_SECONDS,
  tickFormatBasedOnTimeResolution,
  getGraphHeight,
  getTimeResolution
} from 'components/PipelineSummary/RunsGraphHelpers';

require('./RunsHistoryGraph.scss');
require('react-vis/dist/styles/plot.scss');

const MARKSERIESSTROKECOLOR = '#999999';
const FAILEDRUNCOLOR = '#A40403';
const SUCCESSRUNCOLOR = '#3cc801';
const LINECOLOR = '#DBDBDB';
const PREFIX = `features.PipelineSummary.runsHistoryGraph`;
const GRAPHPREFIX = `features.PipelineSummary.graphs`;
const COLORLEGENDS = [
  {
    title: T.translate(`${PREFIX}.legend1`),
    color: FAILEDRUNCOLOR
  },
  {
    title: T.translate(`${PREFIX}.legend2`),
    color: SUCCESSRUNCOLOR
  }
];

export default class RunsHistoryGraph extends Component {
  constructor(props) {
    super(props);
    this.state = {
      data: this.constructData(),
      totalRunsCount: props.totalRunsCount,
      viewState: 'chart',
      currentHoveredElement: null,
      runsLimit: props.runsLimit
    };
    this.renderChart = this.renderChart.bind(this);
    this.renderTable = this.renderTable.bind(this);
  }
  componentDidMount() {
    this.setState({
      data: this.constructData()
    });
  }
  componentWillReceiveProps(nextProps) {
    this.setState({
      data: this.constructData(nextProps),
      totalRunsCount: nextProps.totalRunsCount,
      currentHoveredElement: null,
      runsLimit: nextProps.runsLimit
    });
  }
  constructData(props = this.props) {
    let data = [].concat(props.runs).reverse().map((run, id) => {
      let {totalRunsCount, runsLimit, xDomainType} = this.props;
      let x;
      if (xDomainType === 'limit') {
        x = totalRunsCount > runsLimit ? totalRunsCount - runsLimit : 0;
        x = x + (id + 1);
      }
      if (xDomainType === 'time') {
        x = run.start;
      }
      return {
        x,
        y: run.duration,
        color: run.status === 'FAILED' ? FAILEDRUNCOLOR : SUCCESSRUNCOLOR,
        runid: run.runid
      };
    });
    return data;
  }
  renderEmptyMessage() {
    return (
      <div className="empty-runs-container">
        <h1>
          {
            T.translate(`${GRAPHPREFIX}.emptyMessage`, {
              filter: this.props.xDomainType === 'time' ? `in ${this.props.activeFilterLabel}` : ''
            })
          }
        </h1>
      </div>
    );
  }
  renderChart() {
    let FPlot = makeWidthFlexible(XYPlot);
    let height = getGraphHeight(this.containerRef);
    let maxYDomain = Number.MAX_SAFE_INTEGER, minYDomain = {y: 0};
    if (this.state.data.length > 1) {
      maxYDomain = this.state.data.reduce((prev, curr) => {
        return (prev.y > curr.y) ? prev : curr;
      });
      minYDomain = this.state.data.reduce((prev, curr) => {
        return (prev.y < curr.y) ? prev : curr;
      });
    }
    if (this.state.data.length == 1) {
      maxYDomain = this.state.data[0];
    }
    let yAxisResolution = getTimeResolution(maxYDomain.y);
    let xDomain = [];
    if (this.state.data.length > 0) {
      xDomain = getXDomain(this.props);
    }
    let popOverData;
    if (this.state.currentHoveredElement) {
      popOverData = this.props.runs.find(run => this.state.currentHoveredElement.runid === run.runid);
    }
    if (this.props.isLoading) {
      return (
        <div className="empty-runs-container">
          <IconSVG
            name="icon-spinner"
            className="fa-spin"
          />
        </div>
      );
    }
    if (!this.props.runs.length) {
      return this.renderEmptyMessage();
    }
    return (
      <div className="graph-plot-container">
        <FPlot
          xType="linear"
          xDomain={xDomain}
          height={height}
          className="run-history-fp-plot"
        >
          <DiscreteColorLegend
            style={{position: 'absolute', left: '40px', top: '0px'}}
            orientation="horizontal"
            items={COLORLEGENDS}
          />
          <XAxis
            tickTotal={getTicksTotal(this.props)}
            tickFormat={xTickFormat(this.props)}
          />
          <YAxis
            tickTotal={10}
            yDomain={[minYDomain.y, maxYDomain.y]}
            tickFormat={tickFormatBasedOnTimeResolution(yAxisResolution)}
          />
          <HorizontalGridLines />
          <LineSeries
            color={LINECOLOR}
            data={this.state.data}
          />
          <MarkSeries
            data={this.state.data}
            colorType={'literal'}
            stroke={MARKSERIESSTROKECOLOR}
            onValueClick={(d) => {
              if (isEqual(this.state.currentHoveredElement || {}, d)) {
                this.setState({
                  currentHoveredElement: null
                });
              } else {
                this.setState({
                  currentHoveredElement: d
                });
              }
            }}
          />

          {
            this.state.currentHoveredElement && popOverData ?
              (
                <Hint value={this.state.currentHoveredElement}>
                  <h4>{T.translate(`${PREFIX}.hint.title`)}</h4>
                  <div className="log-stats">
                    <div>
                      <span>{T.translate(`${PREFIX}.hint.duration`)}</span>
                      <span>
                        {
                          popOverData.duration < ONE_MIN_SECONDS ?
                            `${popOverData.duration} seconds`
                          :
                            moment.duration(popOverData.duration, yAxisResolution)
                        }
                      </span>
                    </div>
                    <div>
                      <span>{T.translate(`${PREFIX}.hint.status`)}</span>
                      <span className={classnames({
                        'text-danger': ['FAILED', 'KILLED', 'RUN_FAILED'].indexOf(popOverData.status) !== -1,
                        'text-success': ['FAILED', 'KILLED', 'RUN_FAILED'].indexOf(popOverData.status) === -1
                      })}>{popOverData.status}</span>
                    </div>
                  </div>
                  {
                    this.props.xDomainType === 'limit' ?
                      <div>
                        <strong>{T.translate(`${PREFIX}.hint.runNumber`)}: </strong>
                        <span>{this.state.currentHoveredElement.x}</span>
                      </div>
                    :
                      null
                  }
                  <div>
                    <strong>{T.translate(`${PREFIX}.hint.startTime`)}: </strong>
                    <span>{moment(popOverData.start * 1000).format('llll')}</span>
                  </div>
                </Hint>
              )
            :
              null
          }
          {
            this.props.xDomainType === 'limit' ?
              <div className="x-axis-title"> {T.translate(`${PREFIX}.xAxisTitle`)} </div>
            :
              null
          }
          <div className="y-axis-title">{T.translate(`${PREFIX}.yAxisTitle`, {
            resolution: yAxisResolution
          })}</div>
        </FPlot>
      </div>
    );
  }
  renderTable() {
    if (!this.props.runs.length) {
      return this.renderEmptyMessage();
    }
    return (
      <div className="table-container">
        <table className="table">
          <thead>
            <tr>
              <th>{T.translate(`${PREFIX}.table.headers.runCount`)}</th>
              <th>{T.translate(`${PREFIX}.table.headers.status`)}</th>
              <th>{T.translate(`${PREFIX}.table.headers.startTime`)}</th>
              <th>{T.translate(`${PREFIX}.table.headers.duration`)}</th>
            </tr>
          </thead>
        </table>
        <div className="table-scroll">
          <table className="table">
            <tbody>
              {
                this.props.runs.map((run, i) => {
                  return (
                    <tr>
                      <td> {i+1} </td>
                      <td>
                        <span className={classnames({
                          'text-danger': ['FAILED', 'KILLED', 'RUN_FAILED'].indexOf(run.status) !== -1,
                          'text-success': ['FAILED', 'KILLED', 'RUN_FAILED'].indexOf(run.status) === -1
                        })}>
                          {run.status}
                        </span>
                      </td>
                      <td> {moment(run.start).format('llll')}</td>
                      <td> {run.duration}</td>
                    </tr>
                  );
                })
              }
            </tbody>
          </table>
        </div>
      </div>
    );
  }
  render() {
    return (
      <div
        className="runs-history-graph"
        ref={ref => this.containerRef = ref}
      >
        <div className="title-container">
          <div className="title">{T.translate(`${PREFIX}.title`)} </div>
          <div className="viz-switcher">
            <span
              className={classnames({"active": this.state.viewState === 'chart'})}
              onClick={() => this.setState({viewState: 'chart'})}
            >
              {T.translate(`${GRAPHPREFIX}.vizSwitcher.chart`)}
            </span>
            <span
              className={classnames({"active": this.state.viewState === 'table'})}
              onClick={() => this.setState({viewState: 'table'})}
            >
              {T.translate(`${GRAPHPREFIX}.vizSwitcher.table`)}
            </span>
          </div>
        </div>
        {
          this.state.viewState === 'chart' ?
            this.renderChart()
          :
            this.renderTable()
        }
      </div>
    );
  }
}
RunsHistoryGraph.propTypes = {
  runs: PropTypes.arrayOf(PropTypes.object),
  totalRunsCount: PropTypes.number,
  runsLimit: PropTypes.number,
  xDomainType: PropTypes.oneOf(['limit', 'time']),
  runContext: PropTypes.object,
  isLoading: PropTypes.bool,
  start: PropTypes.number,
  end: PropTypes.number,
  activeFilterLabel: PropTypes.string
};
