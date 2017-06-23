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

require('./RunsHistoryGraph.scss');
require('react-vis/dist/styles/plot.scss');

const FAILEDRUNCOLOR = '#A40403';
const SUCCESSRUNCOLOR = '#3cc801';
const LINECOLOR = '#DBDBDB';
const PREFIX = `features.PipelineSummary.runsHistoryGraph`;
const GRAPHPREFIX = `features.PipelineSummary.graphs`;

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
      return {
        x: this.props.xDomainType === 'limit' ? id + 1 : run.start,
        y: run.duration,
        fill: run.status === 'FAILED' ? FAILEDRUNCOLOR : SUCCESSRUNCOLOR,
        color: run.status === 'FAILED' ? FAILEDRUNCOLOR : SUCCESSRUNCOLOR,
        runid: run.runid
      };
    });
    return data;
  }
  renderChart() {
    let FPlot = makeWidthFlexible(XYPlot);
    let height = 300;
    if (this.containerRef) {
      let clientRect = this.containerRef.getBoundingClientRect();
      height = clientRect.height - 100;
    }
    let maxYDomain = Number.MAX_SAFE_INTEGER, minYDomain = 0;
    if (this.state.data.length > 1) {
      maxYDomain = this.state.data.reduce((prev, curr) => {
        return (prev.y > curr.y) ? prev : curr;
      });
      minYDomain = this.state.data.reduce((prev, curr) => {
        return (prev.y < curr.y) ? prev : curr;
      });
    }
    if (this.state.data.length == 1) {
      maxYDomain = this.state.data[0].y;
    }
    let xDomain = [1, this.state.runsLimit];
    if (this.props.xDomainType === 'time' && this.state.data.length > 0) {
      xDomain = [this.state.data[0].x, this.state.data[this.state.data.length - 1].x];
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
      return (
        <div className="empty-runs-container">
          <h1> {T.translate(`${GRAPHPREFIX}.emptyMessage`)} </h1>
        </div>
      );
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
            orientation="horizontal" items={[
              {
                title: T.translate(`${PREFIX}.legend1`),
                color: FAILEDRUNCOLOR
              },
              {
                title: T.translate(`${PREFIX}.legend2`),
                color: SUCCESSRUNCOLOR
              }
            ]}
          />
          <HorizontalGridLines />
          <LineSeries
            color={LINECOLOR}
            data={this.state.data}
          />
          <MarkSeries
            data={this.state.data}
            colorType={'literal'}
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
          <XAxis
            tickTotal={10}
            tickFormat={(v => {
              if (this.props.xDomainType === 'time') {
                return moment(v * 1000).format('ddd M/D/YY');
              }
              return v;
            })}
          />
          <YAxis yDomain={[minYDomain.y, maxYDomain.y]}/>

          {
            this.state.currentHoveredElement && popOverData ?
              <Hint value={this.state.currentHoveredElement}>
                <h4>{T.translate(`${PREFIX}.hint.title`)}</h4>
                <div className="log-stats">
                  <div>
                    <span> {T.translate(`${PREFIX}.hint.duration`)} </span>
                    <span> {popOverData.duration}</span>
                  </div>
                  <div>
                    <span> {T.translate(`${PREFIX}.hint.status`)} </span>
                    <span className={classnames({
                      'text-danger': ['FAILED', 'KILLED', 'RUN_FAILED'].indexOf(popOverData.status) !== -1,
                      'text-success': ['FAILED', 'KILLED', 'RUN_FAILED'].indexOf(popOverData.status) === -1
                    })}> {popOverData.status}</span>
                  </div>
                </div>
                <div>
                  <strong> {T.translate(`${PREFIX}.hint.startTime`)}: </strong>
                  <span>{ moment(popOverData.start * 1000).format('llll')} </span>
                </div>
              </Hint>
            :
              null
          }
          <div className="x-axis-title"> {T.translate(`${PREFIX}.xAxisTitle`)} </div>
          <div className="y-axis-title">{T.translate(`${PREFIX}.yAxisTitle`)}</div>
        </FPlot>
      </div>
    );
  }
  renderTable() {
    if (!this.props.runs.length) {
      return (
        <div className="empty-runs-container">
          <h1> {T.translate(`${GRAPHPREFIX}.emptyMessage`)} </h1>
        </div>
      );
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
  isLoading: PropTypes.bool
};
