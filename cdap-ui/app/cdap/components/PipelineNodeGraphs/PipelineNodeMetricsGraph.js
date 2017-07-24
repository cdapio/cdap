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

const PREFIX = `features.PipelineSummary.pipelineNodesMetricsGraph`;

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
    metrics: PropTypes.arrayOf(PropTypes.string)
  };

  state = {
    recordsInData: [],
    recordsOutData: [],
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
    let totalRecordsIn = 0, totalRecordsOut = 0;
    let recordsInData = data.series.find(d => recordsInRegex.test (d.metricName)) || [];
    let recordsOutData = data.series.find(d => recordsOutRegex.test(d.metricName)) || [];
    if (Array.isArray(recordsInData.data)) {
      recordsInData = recordsInData.data.map((d) => {
        totalRecordsIn += d.value;
        return {
          x: d.time,
          y: totalRecordsIn
        };
      });
      recordsInData = getGapFilledAccumulatedData(recordsInData).map((data, i) => ({
        x: i,
        y: data.y
      }));
    }
    if (Array.isArray(recordsOutData.data)) {
      recordsOutData = recordsOutData.data.map((d) => {
        totalRecordsOut += d.value;
        return {
          x: d.time,
          y: totalRecordsOut
        };
      });
      recordsOutData = getGapFilledAccumulatedData(recordsOutData).map((data, i) => ({
        x: i,
        y: data.y
      }));
    }
    this.setState({
      recordsInData,
      recordsOutData,
      resolution
    });
  }

  getRecordsInOut({qid: data}) {
    let recordsInRegex = new RegExp(/user.*.records.in/);
    let recordsOutRegex = new RegExp(/user.*.records.out/);
    if (!data.series.length) {
      return {
        recordsInData: null,
        recordsOutData: null
      };
    }
    let recordsInData = data.series.find(d => recordsInRegex.test (d.metricName)) || {};
    let recordsOutData = data.series.find(d => recordsOutRegex.test(d.metricName)) || {};
    recordsInData = (recordsInData.data || []).length ? recordsInData.data[0].value : 0;
    recordsOutData = (recordsOutData.data || []).length ? recordsOutData.data[0].value : 0;

    return {
      recordsInData,
      recordsOutData
    };
  }

  fetchData = () => {
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
    if (!data.length) {
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

    return (
      <div className="node-metrics-single-datapoint">
        {data}
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
  renderContent() {
    if (this.state.loading) {
      return (
        <LoadingSVGCentered />
      );
    }
    let runNumber = findIndex([...this.props.runContext.runs].reverse(), {runid: this.props.runContext.runRecord.runid}) + 1;
    return (
      <div className="node-metrics-container">
        <div ref={ref => this.containerRef = ref}>
          <div className="title-container">
            <div className="title"> {T.translate(`${PREFIX}.recordsInTitle`, {runNumber})} </div>
          </div>
          {this.renderMetrics(this.state.recordsInData, 'recordsin')}
        </div>
        <div>
          <div className="title-container">
            <div className="title"> {T.translate(`${PREFIX}.recordsOutTitle`, {runNumber})} </div>
          </div>
          {this.renderMetrics(this.state.recordsOutData, 'recordsout')}
        </div>
      </div>
    );
  }
  render() {
    return (
      <div className="pipeline-node-metrics-graph">
        {this.renderContent()}
      </div>
    );
  }
}
