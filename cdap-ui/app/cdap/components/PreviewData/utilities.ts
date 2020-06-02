/*
 * Copyright © 2020 Cask Data, Inc.
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

import { MyPreviewApi } from 'api/preview';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { Map, List } from 'immutable';
import { IStageSchema } from 'components/AbstractWidget';
import { IPluginProperty } from 'components/ConfigurationGroup/types';
import capitalize from 'lodash/capitalize';
import isEmpty from 'lodash/isEmpty';

export interface IConnection {
  from: string;
  to: string;
  port?: string;
}

export interface INode {
  plugin?: any;
  isSource?: boolean;
  isSink?: boolean;
  isCondition?: boolean;
}

export interface IStage {
  inputSchema: IStageSchema[] | string;
  name: string;
  outputSchema: IStageSchema[] | string;
  plugin: IPluginProperty;
}

type IAdjacencyMap = Map<string, List<string>>;

export interface IRecords {
  records?: any[];
  schemaFields?: string[];
}

export interface IPreviewData {
  numInputStages?: number;
  input?: IRecords;
  numOutputStages?: number;
  output?: IRecords;
}

// TO DO: Add unit tests
export function getAdjacencyMap(connections: IConnection[]) {
  let adjacencyMap: IAdjacencyMap = Map();
  if (!Array.isArray(connections)) {
    return adjacencyMap;
  }
  connections.forEach((conn) => {
    if (adjacencyMap.has(conn.from)) {
      const existingConnections = adjacencyMap.get(conn.from);
      const newConnections = existingConnections.push(conn.to);
      adjacencyMap = adjacencyMap.set(conn.from, newConnections);
    } else {
      adjacencyMap = adjacencyMap.set(conn.from, List([conn.to]));
    }
  });
  return adjacencyMap;
}

export function fetchPreview(
  selectedNode: INode,
  previewId: string,
  stages: IStage[],
  connections: IConnection[],
  loadingPreviewCb: (isLoading: boolean) => void,
  updatePreviewCb: (previewData: IPreviewData) => void
) {
  loadingPreviewCb(true);

  const params = {
    namespace: getCurrentNamespace(),
    previewId,
  };

  const selectedStageName = selectedNode.plugin.label;

  const adjacencyMap = getAdjacencyMap(connections);
  const { tracers, previousStagePort, previousStage } = getTracersAndPreviousStageInfo(
    selectedStageName,
    adjacencyMap,
    stages,
    connections
  );

  const postBody = {
    tracers,
  };

  MyPreviewApi.getStageData(params, postBody).subscribe(
    (res) => {
      const previewData = getRecords(res, selectedNode, previousStage, previousStagePort);
      updatePreviewCb(previewData);
      loadingPreviewCb(false);
    },
    (err) => {
      // TO DO: error handling here
      // tslint:disable-next-line: no-console
      console.log('Error loading preview data: ', err);
      loadingPreviewCb(false);
    }
  );
}

export function getPreviousStageName(selectedStage: string, adjMap: IAdjacencyMap) {
  const keys = adjMap.keySeq().toArray();
  const prevStageName = keys.find((key) => adjMap.get(key).includes(selectedStage));
  return prevStageName;
}

function getPreviousStage(previousStageName: string, stages: IStage[]) {
  return stages.find((stage) => stage.name === previousStageName);
}

function getTracersAndPreviousStageInfo(
  selectedStage: string,
  adjacencyMap: IAdjacencyMap,
  stages: IStage[],
  connections: IConnection[]
) {
  let tracers: List<string> = List([selectedStage]);
  let previousStageName = getPreviousStageName(selectedStage, adjacencyMap);

  // source nodes have no previous stage
  if (!previousStageName) {
    return { tracers: tracers.toArray() };
  }

  let previousStage = getPreviousStage(previousStageName, stages);
  let previousStagePort;

  if (previousStage.plugin.type === 'splittertransform') {
    const previousStageConnection = connections.find(
      (connection) => connection.from === previousStageName && connection.to === selectedStage
    );
    if (previousStageConnection) {
      previousStagePort = previousStageConnection.port;
    }
  } else {
    // If we have multiple condition nodes in a row, we have traverse back
    // until we find a node that actually has records out
    while (previousStage && previousStage.plugin.type === 'condition') {
      previousStageName = getPreviousStageName(previousStageName, adjacencyMap);
      previousStage = stages.find((stage) => stage.name === previousStageName);
    }
  }
  tracers = tracers.push(previousStageName);
  return {
    tracers: tracers.toArray(),
    previousStage,
    previousStagePort,
  };
}

function getRecords(res, selectedNode: INode, previousStage, previousStagePort) {
  const selectedStageName = selectedNode.plugin.label;
  const isSource = selectedNode.isSource;
  const isSink = selectedNode.isSink;

  const previewData: IPreviewData = {
    input: {},
    output: {},
    numInputStages: 0,
    numOutputStages: 0,
  };

  const recordsOut: IRecords = {};
  const recordsIn: IRecords = {};

  Object.entries(res).forEach(([stageName, stageMetrics]) => {
    const recordsOutPorts = Object.keys(stageMetrics).filter(
      (metricName) => metricName.indexOf('records.out.') !== -1
    );

    // Look at the metrics of the stage that user clicked on
    // i.e. set recordsOut to the value returned at the 'records.out' property
    if (stageName === selectedStageName) {
      if (recordsOutPorts.length) {
        recordsOutPorts.forEach((recordsOutPort) => {
          const portName = capitalize(recordsOutPort.split('.').pop());
          recordsOut[portName] = formatMultipleRecords(stageMetrics[recordsOutPort]);
        });
      } else {
        recordsOut[stageName] = formatMultipleRecords(stageMetrics['records.out']);
      }

      // Get the metrics of the stage previous to the one the user clicked on
      // set the recordsIn of current stage to recordsOut of previous stage
    } else {
      let correctMetricsName;
      if (recordsOutPorts.length) {
        correctMetricsName = recordsOutPorts.find(
          (port) => port.split('.').pop() === previousStagePort
        );
      } else if (
        stageMetrics.hasOwnProperty('records.alert') &&
        selectedNode.plugin.type === 'alertpublisher'
      ) {
        correctMetricsName = 'records.alert';
      } else {
        correctMetricsName = 'records.out';
      }
      recordsIn[stageName] = formatMultipleRecords(stageMetrics[correctMetricsName]);
    }
  });

  if (!isSink) {
    previewData.output = recordsOut;
    previewData.numOutputStages = Object.keys(recordsOut).length;
  }
  if (!isSource) {
    previewData.input = recordsIn;
    previewData.numInputStages = Object.keys(recordsIn).length;
  }

  // set current stage's input records to null only if its output records are null AND if the previous
  // stage is a Condition, since that means data didn't flow through to that branch
  const previousStageIsCondition = previousStage && previousStage.plugin.type === 'conditoin';

  if (isEmpty(recordsOut[Object.keys(recordsOut)[0]]) && previousStageIsCondition) {
    previewData.input = {};
    previewData.numInputStages = 0;
  }
  return previewData;
}

function formatMultipleRecords(records) {
  const mapInputs: IRecords = {
    schemaFields: [],
    records: [],
  };
  if (isEmpty(records)) {
    return mapInputs;
  }

  records.forEach((record) => {
    let json = record;
    if (json.value) {
      json = json.value;
    }
    let schemaFields;
    let data;

    if (json.schema) {
      schemaFields = json.schema.fields.map((field) => {
        return field.name;
      });
    } else {
      schemaFields = Object.keys(json);
    }

    if (json.fields) {
      data = json.fields;
    } else {
      data = json;
    }

    mapInputs.schemaFields = schemaFields;
    mapInputs.records.push(data);
  });

  return mapInputs;
}
