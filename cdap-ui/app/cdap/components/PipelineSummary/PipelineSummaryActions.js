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

import {MyProgramApi} from 'api/program';
import PipelineSummaryStore, {PIPELINESSUMMARYACTIONS} from 'components/PipelineSummary/PipelineSummaryStore';
import {MyMetricApi} from 'api/metric';
import {convertProgramToMetricParams} from 'services/program-api-converter';
import {objectQuery} from 'services/helpers';
import isNil from 'lodash/isNil';

function setRuns(runs) {
  PipelineSummaryStore.dispatch({
    type: PIPELINESSUMMARYACTIONS.SETRUNS,
    payload: {
      runs: runs.map(run => ({
        runid: run.runid,
        start: run.start,
        end: run.end,
        duration: isNil(run.end) ? (Math.ceil(Date.now()/1000) - run.start) : (run.end - run.start),
        status: run.status
      }))
    }
  });
}

function fetchMetrics({namespaceId, appId, programType, programId}, metrics) {
  let {runs} = PipelineSummaryStore.getState().pipelinerunssummary;
  let postBody = {};
  runs.forEach((run) => {
    postBody[`qid_${run.runid}`] = {
      tags: {
        namespace: namespaceId,
        app: appId,
        [convertProgramToMetricParams(programType)]: programId,
        run: run.runid
      },
      metrics,
      timeRange: {
        aggregate: 'true'
      }
    };
  });
  return MyMetricApi.query(null, postBody);
}

function updateMetrics(metrics, mapKey = 'metrics') {
  let modMetrics = {};
  Object
    .keys(metrics)
    .forEach(key => {
      let runId = key.replace('qid_', '');
      modMetrics[runId] = {[mapKey]: {}};
      metrics[key]
        .series
        .forEach(metricObj => {
          modMetrics[runId][mapKey][metricObj.metricName] = objectQuery(metricObj, 'data', 0, 'value');
        });
    });
  let {runs} = PipelineSummaryStore.getState().pipelinerunssummary;
  runs = runs.map(run => {
    let runid = run.runid;
    return Object.assign({}, run, modMetrics[runid]);
  });
  PipelineSummaryStore.dispatch({
    type: PIPELINESSUMMARYACTIONS.SETRUNS,
    payload: {
      runs
    }
  });
}

function fetchSummary({namespaceId, appId, programType, programId, pipelineConfig, limit = -1, start = -1, end = -1}) {
  let params = {
    namespace: namespaceId,
    appId,
    programType,
    programId
  };
  PipelineSummaryStore.dispatch({
    type: PIPELINESSUMMARYACTIONS.SETLOADING,
    payload: {
      loading: true
    }
  });
  const addProperty = (propertyValue, propertyName) => {
    if (propertyValue === -1) {
      return {};
    }
    return {[propertyName]: propertyValue};
  };
  params = Object.assign({}, params, addProperty(limit, 'limit'), addProperty(start, 'start'), addProperty(end, 'end'));
  MyProgramApi
    .runs(params)
    .subscribe((runs) => {
      setRuns(runs);
      let logMetrics = [
        'system.app.log.error',
        'system.app.log.warn',
        'system.app.log.info',
        'system.app.log.debug',
        'system.app.log.trace'
      ];
      let nodeMetrics = [];
      pipelineConfig.config.stages.forEach(node => {
        nodeMetrics = nodeMetrics.concat([`user.${node.name}.records.in`, `user.${node.name}.records.out`]);
      });
      fetchMetrics(params, logMetrics)
        .subscribe(logsMetrics => updateMetrics(logsMetrics, 'logsMetrics'));
      fetchMetrics(params, nodeMetrics)
        .subscribe(nodesMetrics => updateMetrics(nodesMetrics, 'nodesMetrics'));
    });
}

export {fetchSummary};
