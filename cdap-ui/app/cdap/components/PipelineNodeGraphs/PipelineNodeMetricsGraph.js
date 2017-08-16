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

import React, {PropTypes, Component} from 'react';
import {MyMetricApi} from 'api/metric';
import T from 'i18n-react';
import Rx from 'rx';
import EmptyMessageContainer from 'components/PipelineSummary/EmptyMessageContainer';
import NodeMetricsGraph from 'components/PipelineNodeGraphs/NodeMetricsGraph';
import isNil from 'lodash/isNil';
require('./PipelineNodeMetricsGraph.scss');
import findIndex from 'lodash/findIndex';
import {getGapFilledAccumulatedData} from 'components/PipelineSummary/RunsGraphHelpers';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import CopyableRunID from 'components/PipelineSummary/CopyableRunID';
import {humanReadableDuration} from 'services/helpers';

const PREFIX = `features.PipelineSummary.pipelineNodesMetricsGraph`;
const RECORDS_IN_PATH_COLOR = '#97A0BA';
const RECORDS_ERROR_PATH_COLOR = '#A40403';
const RECORDS_OUT_PATH_COLOR = '#58B7F6';
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
    plugin: PropTypes.object
  };

  state = {
    recordsInData: [],
    recordsOutData: [],
    recordsErrorData: [],
    totalRecordsIn: null,
    totalRecordsOut: null,
    totalRecordsError: null,
    processTimeMetrics: {},
    resolution: 'hours',
    aggregate: false,
    loading: true
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

  constructData = ({qid: data}) => {
    let resolution = this.getResolution(data.resolution);
    let recordsInRegex = new RegExp(/user.*.records.in/);
    let recordsOutRegex = new RegExp(/user.*.records.out/);
    let recordsErrorRegex = new RegExp(/user.*.records.error/);
    let recordsInData = data.series.find(d => recordsInRegex.test (d.metricName)) || [];
    let recordsOutData = data.series.find(d => recordsOutRegex.test(d.metricName)) || [];
    let recordsErrorData = data.series.find(d => recordsErrorRegex.test(d.metricName)) || [];
    const formatData = (records, numOfDataPoints) => {
      let totalRecords = 0;
      let formattedRecords = [];
      if (Array.isArray(records.data)) {
        formattedRecords = records.data.map((d) => {
          totalRecords += d.value;
          return {
            x: d.time,
            y: totalRecords
          };
        });
        formattedRecords = getGapFilledAccumulatedData(formattedRecords, numOfDataPoints)
          .map((data, i) => ({
            x: i,
            y: data.y
          }));
      }
      return formattedRecords;
    };
    let numOfDataPoints = Math.max(
      Array.isArray(recordsOutData.data) ? recordsOutData.data.length : 0,
      Array.isArray(recordsInData.data) ? recordsInData.data.length : 0,
      Array.isArray(recordsErrorData.data) ? recordsErrorData.data.length : 0
    );
    recordsOutData = formatData(recordsOutData, numOfDataPoints);
    recordsInData = formatData(recordsInData, numOfDataPoints);
    recordsErrorData = formatData(recordsErrorData, numOfDataPoints);

    this.setState({
      recordsInData,
      recordsOutData,
      recordsErrorData,
      resolution
    });
  }

  getRecordsInOut({qid: data}) {
    let recordsInRegex = new RegExp(/user.*.records.in/);
    let recordsOutRegex = new RegExp(/user.*.records.out/);
    let recordsErrorRegex = new RegExp(/user.*.records.error/);
    if (!data.series.length) {
      return {
        recordsInData: null,
        recordsOutData: null
      };
    }
    let recordsInData = data.series.find(d => recordsInRegex.test (d.metricName)) || {};
    let recordsOutData = data.series.find(d => recordsOutRegex.test(d.metricName)) || {};
    let recordsErrorData = data.series.find(d => recordsErrorRegex.test(d.metricName)) || {};
    recordsInData = (recordsInData.data || []).length ? recordsInData.data[0].value : 0;
    recordsOutData = (recordsOutData.data || []).length ? recordsOutData.data[0].value : 0;
    recordsErrorData = (recordsErrorData.data || []).length ? recordsErrorData.data[0].value : 0;

    return {
      recordsInData,
      recordsOutData,
      recordsErrorData
    };
  }

  getOutputRecordsForCharting() {
    return {
      'recordsOut': {
        data: this.state.recordsOutData,
        label: T.translate(`${PREFIX}.recordsOutTitle`),
        color: RECORDS_IN_PATH_COLOR
      },
      'recordsError': {
        data: this.state.recordsErrorData,
        label: T.translate(`${PREFIX}.recordsErrorTitle`),
        color: RECORDS_ERROR_PATH_COLOR
      }
    };
  }

  getInputRecordsForCharting () {
    let defaultMap = {
      'recordsIn': {
        data: this.state.recordsInData,
        label: T.translate(`${PREFIX}.recordsInTitle`),
        color: RECORDS_OUT_PATH_COLOR
      }
    };
    if (this.isPluginSink()) {
      return Object.assign({}, defaultMap, {
        'recordsError': {
          data: this.state.recordsErrorData,
          label: T.translate(`${PREFIX}.recordsErrorTitle`),
          color: RECORDS_ERROR_PATH_COLOR
        }
      });
    }
    return defaultMap;
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
          let recordsOut, recordsIn, recordsError;
          let processTimeMetrics = {};
          data.forEach(d => {
            let metricName = d.metricName;
            let specificMetric = REGEXTOLABELLIST.find(metricObj => metricObj.regex.test(d.metricName));
            if (specificMetric) {
              metricName = specificMetric.id;
            }
            if (d.metricName.match(/user.*records.in/)) {
              recordsIn = d.data[0].value;
            } else if (d.metricName.match(/user.*records.out/)) {
              recordsOut = d.data[0].value;
            } else if (d.metricName.match(/user.*.records.error/)) {
              recordsError = d.data[0].value;
            }
            processTimeMetrics[metricName] = d.data[0].value;
          });
          this.setState({
            processTimeMetrics,
            totalRecordsIn: recordsIn,
            totalRecordsOut: recordsOut,
            totalRecordsError: recordsError
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
      .flatMap(
        res => {
          if (res.qid.series.length === 0) {
            postBody.qid.timeRange = {'aggregate': true};
            return MyMetricApi.query(null, postBody);
          }
          this.setState({
            data: this.constructData(res),
            loading: false
          });
          return Rx.Observable.create((observer) => {
            observer.onNext(false);
          });
        }
      ).subscribe(
        res => {
          if (!res) {
            return;
          }
          this.setState({
            aggregate: true,
            ...this.getRecordsInOut(res),
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

  renderChart = (data, type) => {
    if (Array.isArray(data) && !data.length) {
      return <EmptyMessageContainer message={T.translate(`${PREFIX}.nodata`)} />;
    }
    return (
      <NodeMetricsGraph
        xAxisTitle={this.state.resolution}
        yAxisTitle={T.translate(`${PREFIX}.numberOfRecords`)}
        data={data}
        metricType={type}
      />
    );
  }

  renderSingleMetric(data) {
    if (isNil(data)) {
      return <EmptyMessageContainer message={T.translate(`${PREFIX}.nodata`)} />;
    }

    if (!Array.isArray(data) && typeof data === 'object') {
      return (
        <div className="node-metrics-single-datapoint">
          {
            Object.keys(data).map(key => {
              return (
                <span>
                  <small>{data[key].label}</small>
                  <span>{isNil(data[key].data) ? T.translate('commons.notAvailable') : data[key].data}</span>
                </span>
              );
            })
          }
        </div>
      );
    }
    return (
      <div className="node-metrics-single-datapoint">
        {data}
      </div>
    );
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
        if (this.isPluginSource() && metric.match(/user.*.records.in/)) {
          validRecords = this.state.processTimeMetrics[metric];
          return;
        }
        if (this.isPluginSink() && metric.match(/user.*.records.out/)) {
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
      if (time < 1000) {
        return `${parseFloat(time / 1000, 10).toFixed(2)} ${T.translate('commons.milliSecondsShortLabel')}`;
      }
      return humanReadableDuration(
        Math.ceil(this.state.processTimeMetrics[REGEXTOLABELLIST[0].id] / 1000000)
      ) || T.translate('commons.notAvailable');
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
  };

  isPluginSource = () => {
    return ['batchsource', 'realtimesource', 'streamingsource'].indexOf(this.props.plugin.type) !== -1;
  };

  isPluginSink = () => {
    return ['batchsink', 'realtimesink', 'sparksink'].indexOf(this.props.plugin.type) !== -1;
  };

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
      [type]: this.state[type] || T.translate('commons.notAvailable')
    });
  };

  renderContent() {
    return (
      <div className="node-metrics-container">
        {
          this.isPluginSource() ?
            null
          :
            <div>
              <div className="title-container graph-title">
                <div className="title"> {T.translate(`${PREFIX}.recordsInTitle`)}</div>
                <div className="total-records">
                  {
                    this.state.aggregate ?
                      null
                    :
                      <strong>
                        <span>
                          {this.renderRecordsCount('totalRecordsIn')}
                        </span>
                        {
                          this.isPluginSink() ?
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
          this.isPluginSink() ?
            null
          :
            <div>
              <div className="title-container graph-title">
                <div className="title"> {T.translate(`${PREFIX}.recordsOutTitle`)} </div>
                <div className="total-records">
                  {
                    this.state.aggregate ?
                      null
                    :
                      <strong>
                        <span>
                          { this.renderRecordsCount('totalRecordsOut') }
                        </span>
                        <span className="error-records-count">
                          { this.renderRecordsCount('totalRecordsError') }
                        </span>
                      </strong>
                  }
                </div>
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
          <CopyableRunID runid={this.props.runContext.runRecord.runid} />
        </div>
        {this.renderContent()}
        {this.renderProcesstimeTable()}
      </div>
    );
  }
}
