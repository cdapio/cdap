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
import {objectQuery} from 'services/helpers';
import {XYPlot, makeWidthFlexible, XAxis, YAxis, HorizontalGridLines, Hint, DiscreteColorLegend, VerticalBarSeries as BarSeries} from 'react-vis';
import moment from 'moment';
import isEqual from 'lodash/isEqual';
import {convertProgramToApi} from 'services/program-api-converter';
import classnames from 'classnames';
import T from 'i18n-react';
import IconSVG from 'components/IconSVG';

const WARNINGBARCOLOR = '#FDA639';
const ERRORBARCOLOR = '#A40403';
const PREFIX = `features.PipelineSummary.logsMetricsGraph`;
const GRAPHPREFIX = `features.PipelineSummary.graphs`;
const DEFAULT_TICKS_TOTAL = 10;
const DEFAULT_GRAPH_HEIGHT = 300;

require('./LogsMetricsGraph.scss');
/*
   - Better name
   - Name says LogsMetricsGraph but we are passing in runs. Its logs metrics (warning, error, info etc.,) per run. but ugh..
*/
export default class LogsMetricsGraph extends Component {
  constructor(props) {
    super(props);
    this.state = {
      currentHoveredElement: null,
      viewState: 'chart',
      runsLimit: props.runsLimit
    };
  }
  componentWillReceiveProps(nextProps) {
    this.setState({
      currentHoveredElement: null,
      runsLimit: nextProps.runsLimit
    });
  }
  getDataClusters() {
    let warnings = [];
    let errors = [];
    // Clustering by runs. Stack warnings and errors by clusters.
    [].concat(this.props.runs).reverse().forEach((run, i) => {
      warnings.push({
        x: this.props.xDomainType === 'limit' ? i + 1 : run.start,
        y: objectQuery(run, 'logsMetrics', 'system.app.log.warn') || '',
        runid: run.runid
      });
      errors.push({
        x: this.props.xDomainType === 'limit' ? i + 1 : run.start,
        y: objectQuery(run, 'logsMetrics', 'system.app.log.error') || '',
        runid: run.runid
      });
    });
    return {errors, warnings};
  }
  renderChart() {
    let FPlot = makeWidthFlexible(XYPlot);
    let {errors, warnings} = this.getDataClusters();
    let height = DEFAULT_GRAPH_HEIGHT;
    if (this.containerRef) {
      let clientRect = this.containerRef.getBoundingClientRect();
      height = clientRect.height - 100;
    }
    let xDomain = [1, this.state.runsLimit];
    if (this.props.xDomainType === 'time' && errors.length > 0) {
      let totalErrors = errors.length,
          totalWarnings = warnings.length;
      let startDomain = errors[0].x < warnings[0].x ? errors[0].x : warnings[0].x;
      let endDomain = errors[totalErrors - 1].x > warnings[totalWarnings - 1].x ? errors[totalErrors - 1].x : warnings[totalErrors - 1].x;
      xDomain = [startDomain, endDomain];
    }
    let popOverData, logUrl;
    if (this.state.currentHoveredElement) {
      popOverData = this.props.runs.find(run => this.state.currentHoveredElement.runid === run.runid);
      let {namespaceId, appId, programType, programId} = this.props.runContext;
      logUrl = `/logviewer/view?namespace=${namespaceId}&appId=${appId}&programType=${convertProgramToApi(programType)}&programId=${programId}&runId=${popOverData.runid}`;
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
          className="logs-metrics-fp-plot"
          xType="linear"
          xDomain={xDomain}
          stackBy="y"
          height={height}>
          <DiscreteColorLegend
            style={{position: 'absolute', left: '40px', top: '0px'}}
            orientation="horizontal" items={[
              {
                title: T.translate(`${PREFIX}.legend1`),
                color: WARNINGBARCOLOR
              },
              {
                title: T.translate(`${PREFIX}.legend2`),
                color: ERRORBARCOLOR
              }
            ]}
          />
          <HorizontalGridLines />
          <XAxis
            tickTotal={DEFAULT_TICKS_TOTAL}
            tickFormat={(v => {
              if (this.props.xDomainType === 'time') {
                return moment(v * 1000).format('ddd M/D/YY');
              }
              return v;
            })}
          />
          <YAxis
            tickFormat={(v) => {
              if (Math.floor(v) !== v) {
                return;
              }
              return v;
            }}
          />
          <BarSeries
            cluster="runs"
            color={WARNINGBARCOLOR}
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
            data={warnings}/>
          <BarSeries
            cluster="runs"
            color={ERRORBARCOLOR}
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
            data={errors}/>
          {
            this.state.currentHoveredElement && popOverData ?
              (
                <Hint value={this.state.currentHoveredElement}>
                  <h4>{T.translate(`${PREFIX}.hint.title`)} </h4>
                  <div className="log-stats">
                    <div>
                      <span>{T.translate(`${PREFIX}.hint.errors`)}</span>
                      <span className="text-danger">{popOverData.logsMetrics['system.app.log.error']}</span>
                    </div>
                    <div>
                      <span>{T.translate(`${PREFIX}.hint.warnings`)}</span>
                      <span className="text-warning">{popOverData.logsMetrics['system.app.log.warn']}</span>
                    </div>
                    <a href={logUrl} target="_blank">{T.translate(`${PREFIX}.hint.viewLogs`)}</a>
                  </div>
                  <div>
                    <strong>{T.translate(`${PREFIX}.hint.startTime`)}: </strong>
                    <span>{ moment(popOverData.start * 1000).format('llll')}</span>
                  </div>
                </Hint>
              )
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
    let {namespaceId, appId, programType, programId} = this.props.runContext;
    return (
      <div className="table-container">
        <table className="table">
          <thead>
            <tr>
              <th>{T.translate(`${PREFIX}.table.header.runCount`)}</th>
              <th>{T.translate(`${PREFIX}.table.header.errors`)}</th>
              <th>{T.translate(`${PREFIX}.table.header.warnings`)}</th>
              <th></th>
              <th>{T.translate(`${PREFIX}.table.header.startTime`)}</th>
            </tr>
          </thead>
        </table>
        <div className="table-scroll">
          <table className="table">
            <tbody>
              {
                this.props.runs.map((run, i) => {
                  let logUrl = `/logviewer/view?namespace=${namespaceId}&appId=${appId}&programType=${convertProgramToApi(programType)}&programId=${programId}&runId=${run.runid}`;
                  return (
                    <tr>
                      <td>{i+1} </td>
                      <td>
                        <span className="text-danger">{objectQuery(run, 'logsMetrics', 'system.app.log.error')}</span>
                      </td>
                      <td>
                        <span className="text-warning">{objectQuery(run, 'logsMetrics', 'system.app.log.warn')}</span>
                      </td>
                      <td>
                        <a href={logUrl} target="_blank">{T.translate(`${PREFIX}.table.body.viewLog`)} </a>
                      </td>
                      <td> {moment(run.start).format('llll')}</td>
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
        className="logs-metrics-graph"
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
LogsMetricsGraph.propTypes = {
  runs: PropTypes.arrayOf(PropTypes.object),
  totalRunsCount: PropTypes.number,
  runsLimit: PropTypes.number,
  xDomainType: PropTypes.oneOf(['limit', 'time']),
  runContext: PropTypes.object,
  isLoading: PropTypes.bool
};
