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
import {MyMetricApi} from 'api/metric';
import T from 'i18n-react';
import {Observable} from 'rxjs/Observable';
import EmptyMessageContainer from 'components/PipelineSummary/EmptyMessageContainer';
import NodeMetricsGraph, {isDataSeriesHaveSingleDatapoint} from 'components/PipelineNodeGraphs/NodeMetricsGraph';
import isNil from 'lodash/isNil';
import findIndex from 'lodash/findIndex';
import capitalize from 'lodash/capitalize';
import cloneDeep from 'lodash/cloneDeep';
import {getGapFilledAccumulatedData} from 'components/PipelineSummary/RunsGraphHelpers';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import CopyableID from 'components/CopyableID';
import {humanReadableDuration, isPluginSource, isPluginSink} from 'services/helpers';
import NodeMetricsSingleDatapoint from 'components/PipelineNodeGraphs/NodeMetricsSingleDatapoint';

require('./PipelineNodeMetricsGraph.scss');
const PREFIX = `features.PipelineSummary.pipelineNodesMetricsGraph`;
const RECORDS_OUT_PATH_COLOR = '#97A0BA';
const RECORDS_ERROR_PATH_COLOR = '#A40403';
const RECORDS_IN_PATH_COLOR = '#58B7F6';
const RECORDS_OUT_PORTS_PATH_COLORS = [
  '#979FBB',
  '#FFBA01',
  '#5C6788',
  '#FA6600',
  '#7CD2EB',
  '#999999',
  '#FFA727'
];

const REGEXTOLABELLIST = [
  {
    id: 'processmintime',
    regex: /user.*.time.min$/
  },
  {
    id: 'processmaxtime',
    regex: /user.*.time.max$/
  },
  {
    id: 'processavgtime',
    regex: /user.*.time.avg$/
  },
  {
    id: 'processstddevtime',
    regex: /user.*.time.stddev$/
  }
];

export default class PipelineNodeMetricsGraph extends Component {

  static propTypes = {
    runContext: PropTypes.shape({
      runRecord: PropTypes.shape({
        runid: PropTypes.string,
        start: PropTypes.number,
        end: PropTypes.number
      }),
      runs: PropTypes.arrayOf(PropTypes.object),
      namespace: PropTypes.string,
      app: PropTypes.string,
      programType: PropTypes.string,
      programId: PropTypes.string
    }),
    metrics: PropTypes.arrayOf(PropTypes.string),
    plugin: PropTypes.object,
    portsToShow: PropTypes.arrayOf(PropTypes.string)
  };

  state = {
    recordsInData: [],
    recordsOutData: [],
    recordsErrorData: [],
    recordsOutPortsData: [],
    totalRecordsIn: null,
    totalRecordsOut: null,
    totalRecordsError: null,
    totalRecordsOutPorts: null,
    processTimeMetrics: {},
    resolution: 'hours',
    aggregate: false,
    loading: true,
    showPortsRecordsCountPopover: false
  };

  componentDidMount() {
    this.fetchData();
  }

  getResolution(resolution) {
    switch (resolution) {
      case '1s':
        return T.translate(`${PREFIX}.seconds`);
      case '60s':
        return T.translate(`${PREFIX}.minutes`);
      case '3600s':
      default:
        return T.translate(`${PREFIX}.hours`);
    }
  }

  formatData = (records, numOfDataPoints) => {
    let totalRecords = 0;
    let formattedRecords = [];
    if (Array.isArray(records.data)) {
      formattedRecords = records.data.map((d) => {
        totalRecords += d.value;
        return {
          x: d.time,
          y: totalRecords,
          actualRecords: d.value
        };
      });
      formattedRecords = getGapFilledAccumulatedData(formattedRecords, numOfDataPoints)
        .map((data, i) => ({
          x: i,
          y: data.y,
          time: data.x * 1000,
          actualRecords: data.actualRecords
        }));
    }
    return formattedRecords;
  };

  filterData = ({qid: data}) => {
    let resolution = this.getResolution(data.resolution);
    let recordsInRegex = new RegExp(/user.*.records.in/);
    let recordsOutRegex = new RegExp(/user.*.records.out/);
    let recordsErrorRegex = new RegExp(/user.*.records.error/);
    let recordsOutPortsRegex = new RegExp(/user.*.records.out./);

    let recordsInData = data.series.find(d => recordsInRegex.test (d.metricName)) || [];
    let recordsOutData = data.series.find(d => recordsOutRegex.test(d.metricName)) || [];
    let recordsErrorData = data.series.find(d => recordsErrorRegex.test(d.metricName)) || [];
    let recordsOutPortsData = data.series.filter(d => recordsOutPortsRegex.test(d.metricName)) || [];

    let newState = {
      recordsInData,
      recordsErrorData,
      resolution
    };

    if (!recordsOutPortsData.length) {
      newState.recordsOutData = recordsOutData;
    } else {
      newState.recordsOutPortsData = recordsOutPortsData;
    }

    return newState;
  }

  getOutputRecordsForCharting() {
    let {
      recordsOutData,
      recordsErrorData,
      recordsOutPortsData
    } = cloneDeep(this.state);

    if (!this.state.aggregate) {
      recordsOutData = this.formatData(recordsOutData, Array.isArray(recordsOutData) ? recordsOutData.length : 0);
      recordsErrorData = this.formatData(recordsErrorData, Array.isArray(recordsErrorData) ? recordsErrorData.length : 0);
      for (let i = 0; i < recordsOutPortsData.length; i++) {
        recordsOutPortsData[i] = this.formatData(recordsOutPortsData[i], Array.isArray(recordsOutPortsData[i]) ? recordsOutPortsData[i].length : 0);
      }
    } else {
      recordsOutData = (recordsOutData.data || []).length ? recordsOutData.data[0].value : 0;
      recordsErrorData = (recordsErrorData.data || []).length ? recordsErrorData.data[0].value : 0;
      for (let i = 0; i < recordsOutPortsData.length; i++) {
        let portData = recordsOutPortsData[i];
        recordsOutPortsData[i] = (portData.data || []).length ? portData.data[0].value : 0;
      }
    }

    let outputRecordsObj = {
      'recordsError': {
        data: recordsErrorData,
        label: T.translate(`${PREFIX}.recordsErrorTitle`),
        color: RECORDS_ERROR_PATH_COLOR
      }
    };

    if ((typeof recordsOutData === 'number' && recordsOutData !== 0) || (Array.isArray(recordsOutData) && recordsOutData.length)) {
      return {
        ...outputRecordsObj,
        'recordsOut': {
          data: recordsOutData,
          label: T.translate(`${PREFIX}.recordsOutTitle`),
          color: RECORDS_OUT_PATH_COLOR
        }
      };
    }

    for (let i = 0; i < recordsOutPortsData.length; i++) {
      let portData = this.state.recordsOutPortsData[i];
      let portName = capitalize(portData.metricName.split('.').pop());
      outputRecordsObj = {
        ...outputRecordsObj,
        [portName]: {
          data: recordsOutPortsData[i],
          label: portName,
          color: this.state.totalRecordsOutPorts[portName].color
        }
      };
    }
    return outputRecordsObj;
  }

  getInputRecordsForCharting() {
    let {
      recordsInData,
      recordsErrorData
    } = cloneDeep(this.state);

    if (!this.state.aggregate) {
      recordsInData = this.formatData(recordsInData, Array.isArray(recordsInData) ? recordsInData.length : 0);
      recordsErrorData = this.formatData(recordsErrorData, Array.isArray(recordsErrorData) ? recordsErrorData.length : 0);
    } else {
      recordsInData = (recordsInData.data || []).length ? recordsInData.data[0].value : 0;
      recordsErrorData = (recordsErrorData.data || []).length ? recordsErrorData.data[0].value : 0;
    }

    let inputRecordsObj = {
      'recordsIn': {
        data: recordsInData,
        label: T.translate(`${PREFIX}.recordsInTitle`),
        color: RECORDS_IN_PATH_COLOR
      }
    };
    if (isPluginSink(this.props.plugin.type)) {
      return {
        ...inputRecordsObj,
        'recordsError': {
          data: recordsErrorData,
          label: T.translate(`${PREFIX}.recordsErrorTitle`),
          color: RECORDS_ERROR_PATH_COLOR
        }
      };
    }
    return inputRecordsObj;
  }

  fetchProcessTimeMetrics = () => {
    let {namespace, app, programType, programId, runRecord} = this.props.runContext;
    let postBody = {
      qid: {
        metrics: this.props.metrics,
        tags: {
          namespace,
          app,
          [programType]: programId,
          run: runRecord.runid
        },
        timeRange: {
          aggregate: true
        }
      }
    };
    MyMetricApi
      .query(null, postBody)
      .subscribe(
        (res) => {
          let data = res.qid.series;
          let recordsOut, recordsIn, recordsError, recordsOutPorts;
          recordsOut = recordsIn = recordsError = 0;
          recordsOutPorts = {};
          let processTimeMetrics = {};
          data.forEach(d => {
            let metricName = d.metricName;
            let dataValue = (d.data || []).length ? d.data[0].value : 0;
            let specificMetric = REGEXTOLABELLIST.find(metricObj => metricObj.regex.test(d.metricName));
            if (specificMetric) {
              metricName = specificMetric.id;
            }
            if (d.metricName.match(/user.*records.in/)) {
              recordsIn += dataValue;
            } else if (d.metricName.match(/user.*records.out/)) {
              recordsOut += dataValue;
            } else if (d.metricName.match(/user.*.records.error/)) {
              recordsError += dataValue;
            }
            if (d.metricName.match(/user.*records.out./)) {
              let portName = capitalize(d.metricName.split('.').pop());
              let portColor = RECORDS_OUT_PORTS_PATH_COLORS[Object.keys(recordsOutPorts).length % 7];

              recordsOutPorts[portName] = {
                value: dataValue,
                color: portColor
              };
            }
            processTimeMetrics[metricName] = dataValue;
          });
          this.setState({
            processTimeMetrics,
            totalRecordsIn: recordsIn,
            totalRecordsOut: recordsOut,
            totalRecordsError: recordsError,
            totalRecordsOutPorts: recordsOutPorts
          });
        }
      );
  };

  fetchData = () => {
    this.fetchProcessTimeMetrics();
    let {namespace, app, programType, programId, runRecord} = this.props.runContext;
    let postBody = {
      qid: {
        metrics: this.props.metrics,
        tags: {
          namespace,
          app,
          [programType]: programId,
          run: runRecord.runid
        },
        timeRange: {
          start: 0,
          end: 'now'
        }
      }
    };
    MyMetricApi
      .query(null, postBody)
      .mergeMap(
        res => {
          if (res.qid.series.length === 0) {
            postBody.qid.timeRange = {'aggregate': true};
            return MyMetricApi.query(null, postBody);
          }
          this.setState({
            ...this.filterData(res),
            loading: false
          });
          return Observable.create((observer) => {
            observer.next(false);
          });
        }
      ).subscribe(
        res => {
          if (!res) {
            return;
          }
          this.setState({
            aggregate: true,
            ...this.filterData(res),
            loading: false
          });
        },
        err => {
          this.setState({
            error: typeof err === 'object' ? JSON.stringify(err) : err
          });
        }
      );
  };

  togglePortsRecordsCountPopover = () => {
    this.setState({
      showPortsRecordsCountPopover: !this.state.showPortsRecordsCountPopover
    });
  };

  renderChart = (data, type) => {
    if (Array.isArray(data) && !data.length) {
      return <EmptyMessageContainer message={T.translate(`${PREFIX}.nodata`)} />;
    }

    let isMultiplePorts = type === 'recordsout' && Object.keys(this.state.totalRecordsOutPorts).length > 0;

    return (
      <NodeMetricsGraph
        xAxisTitle={this.state.resolution}
        yAxisTitle={T.translate(`${PREFIX}.numberOfRecords`)}
        data={data}
        metricType={type}
        isMultiplePorts={isMultiplePorts}
        portsToShow={this.props.portsToShow}
      />
    );
  }

  renderSingleMetric(data) {
    if (isNil(data)) {
      return <EmptyMessageContainer message={T.translate(`${PREFIX}.nodata`)} />;
    }

    return <NodeMetricsSingleDatapoint data={data} />;
  }

  /*
    We get processing time (min, max and stddev) metrics in microseconds.
    The calculation here is
    if time < 1000 then show it as milliseconds
    if time > 1000 then show it in human readable duration

    Process rate: Records out/Total time
    Avg Processing Time: 1 / Process rate
  */
  renderProcesstimeTable = () => {
    let totalProcessingTime, validRecords;
    Object
      .keys(this.state.processTimeMetrics)
      .forEach(metric => {
        if (metric.match(/user.*.process.time.total/)) {
          totalProcessingTime = this.state.processTimeMetrics[metric] / 1000000;
        }
        if (isPluginSource(this.props.plugin.type) && metric.match(/user.*.records.in/)) {
          validRecords = this.state.processTimeMetrics[metric];
          return;
        }
        if (isPluginSink(this.props.plugin.type) && metric.match(/user.*.records.out/)) {
          validRecords = this.state.processTimeMetrics[metric];
          return;
        }
        if (metric.match(/user.*.records.out/)) {
          validRecords = this.state.processTimeMetrics[metric];
        }
      });
    let processRate, avgProcessingTime;
    if (isNil(validRecords) || isNil(totalProcessingTime)) {
      processRate = T.translate('commons.notAvailable');
      avgProcessingTime = T.translate('commons.notAvailable');
    } else {
      processRate = parseFloat(validRecords / totalProcessingTime, 10).toFixed(3);
      avgProcessingTime = ((1 / processRate) * 1000000); // Because total processing time is in seconds
    }
    const renderMicroSeconds = (time) => {
      if (time === 0) {
        return `${time} ${T.translate('commons.secondsShortLabel')}`;
      }
      if (time < 1000000) {
        return `${parseFloat(time / 1000, 10).toFixed(6)} ${T.translate('commons.milliSecondsShortLabel')}`;
      }
      return humanReadableDuration(time / 1000000) || T.translate('commons.notAvailable');
    };
    return (
      <div className="process-time-table-container">
        <table className="table table-sm">
          <thead>
            <tr>
              <th>{T.translate(`${PREFIX}.processTimeTable.recordOutPerSec`)}</th>
              <th>{T.translate(`${PREFIX}.processTimeTable.minProcessTime`)}</th>
              <th>{T.translate(`${PREFIX}.processTimeTable.maxProcessTime`)}</th>
              <th>{T.translate(`${PREFIX}.processTimeTable.stddevProcessTime`)}</th>
              <th>{T.translate(`${PREFIX}.processTimeTable.avgProcessTime`)}</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>{processRate}</td>
              <td>{renderMicroSeconds(this.state.processTimeMetrics[REGEXTOLABELLIST[0].id])}</td>
              <td>{renderMicroSeconds(this.state.processTimeMetrics[REGEXTOLABELLIST[1].id])}</td>
              <td>{renderMicroSeconds(this.state.processTimeMetrics[REGEXTOLABELLIST[2].id])}</td>
              <td>{renderMicroSeconds(avgProcessingTime)}</td>
            </tr>
          </tbody>
        </table>
      </div>
    );
  }

  renderMetrics(data, type) {
    if (this.state.aggregate) {
      return this.renderSingleMetric(data);
    }

    return (
      <div className="graph-container">
        {this.renderChart(data, type)}
      </div>
    );
  }

  renderRecordsCount = (type) => {
    return T.translate(`${PREFIX}.${type}`, {
      [type]: this.state[type] || '0'
    });
  };

  renderPortsRecordsCount() {
    if (Object.keys(this.state.totalRecordsOutPorts).length <= 2) {
      return (
        Object.keys(this.state.totalRecordsOutPorts)
          .map(key => {
            return (
              <span>
                { this.renderPortCount(key) }
              </span>
            );
          })
      );
    }

    return (
      <a
        className="toggle-records-count-popover"
        onClick={this.togglePortsRecordsCountPopover}
      >
        {
          this.state.showPortsRecordsCountPopover ?
            <span>{T.translate(`${PREFIX}.portRecordsCountPopover.hide`)}</span>
          :
            <span>{T.translate(`${PREFIX}.portRecordsCountPopover.view`)}</span>
        }
      </a>
    );
  }

  rendePortsRecordsCountPopover() {
    if (!this.state.showPortsRecordsCountPopover) {
      return null;
    }

    return (
      <div className="port-records-count-popover">
        <div className="popover-content">
          <strong>{T.translate(`${PREFIX}.portRecordsCountPopover.title`)}</strong>
          {
            Object.keys(this.state.totalRecordsOutPorts)
              .map(key => {
                let colorStyle = { backgroundColor: this.state.totalRecordsOutPorts[key].color };
                return (
                  <div className="port-count-container">
                    <span
                      className="port-legend-circle"
                      style={colorStyle}
                    />
                    { this.renderPortCount(key) }
                  </div>
                );
              })
          }
          {/* When we show all the ports metrics instead of just View All/Hide All, the Total Errors
          might overflow and be hidden. This is to show the user in case that happens */}
          {
            Object.keys(this.state.totalRecordsOutPorts).length <= 2 ?
              (
                <strong className="error-records-count">
                  {this.renderRecordsCount('totalRecordsError')}
                </strong>
              )
            :
              null
          }
        </div>
      </div>
    );
  }

  renderPortCount = (port) => {
    return T.translate(`${PREFIX}.totalRecordsOutPorts`, {
      port,
      recordCount: this.state.totalRecordsOutPorts[port].value || '0'
    });
  };

  renderContent() {
    let numPorts = Object.keys(this.state.totalRecordsOutPorts).length;
    let onHoverFn = numPorts > 0 && numPorts <= 2 ? this.togglePortsRecordsCountPopover : null;

    return (
      <div className="node-metrics-container">
        {
          isPluginSource(this.props.plugin.type) ?
            null
          :
            <div>
              <div className="title-container graph-title">
                <div className="title"> {T.translate(`${PREFIX}.recordsInTitle`)}</div>
                <div className="total-records">
                  {
                    this.state.aggregate || isDataSeriesHaveSingleDatapoint(this.getInputRecordsForCharting()) ?
                      null
                    :
                      <strong>
                        <span>
                          {this.renderRecordsCount('totalRecordsIn')}
                        </span>
                        {
                          isPluginSink(this.props.plugin.type) ?
                            <span className="error-records-count">
                              {this.renderRecordsCount('totalRecordsError')}
                            </span>
                          :
                            null
                        }
                      </strong>
                  }
                </div>
              </div>
              {this.renderMetrics(this.getInputRecordsForCharting(), 'recordsin')}
            </div>
        }
        {
          isPluginSink(this.props.plugin.type) ?
            null
          :
            <div>
              <div className="title-container graph-title">
                <div className="title"> {T.translate(`${PREFIX}.recordsOutTitle`)} </div>
                <div
                  className="total-records"
                  onMouseEnter={onHoverFn}
                  onMouseLeave={onHoverFn}
                >
                  {
                    this.state.aggregate || isDataSeriesHaveSingleDatapoint(this.getOutputRecordsForCharting()) ?
                      null
                    :
                      <span>
                        <strong>
                          { this.renderRecordsCount('totalRecordsOut') }
                        </strong>
                        { this.renderPortsRecordsCount() }
                        <strong className="error-records-count">
                          { this.renderRecordsCount('totalRecordsError') }
                        </strong>
                      </span>
                  }
                </div>
                { this.rendePortsRecordsCountPopover() }
              </div>
              {this.renderMetrics(this.getOutputRecordsForCharting(), 'recordsout')}
            </div>
        }
      </div>
    );
  }

  render() {
    if (this.state.loading) {
      return (
        <LoadingSVGCentered />
      );
    }
    let runNumber = findIndex([...this.props.runContext.runs].reverse(), {runid: this.props.runContext.runRecord.runid}) + 1;
    return (
      <div className="pipeline-node-metrics-graph">
        <div className="title-container">
          <div className="title">
            {
              T.translate(`${PREFIX}.runOfTitle`, {
                runNumber,
                totalRun: this.props.runContext.runs.length
              })
            }
          </div>
          <CopyableID id={this.props.runContext.runRecord.runid} />
        </div>
        {this.renderContent()}
        {this.renderProcesstimeTable()}
      </div>
    );
  }
}
