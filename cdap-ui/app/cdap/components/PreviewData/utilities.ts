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
import { objectQuery } from 'services/helpers';

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

// Stage info from ConfigStore
export interface IStage {
  inputSchema: IStageSchema[] | string;
  name: string;
  outputSchema: IStageSchema[] | string;
  plugin: IPluginProperty;
}

// Previous stage(s) info compiled by UI
interface IPreviousStageInfo {
  [key: string]: {
    port?: string;
    condition?: boolean;
  };
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
  updatePreviewCb: (previewData: IPreviewData) => void,
  errorCb?: (err: any) => void
) {
  loadingPreviewCb(true);

  const params = {
    namespace: getCurrentNamespace(),
    previewId,
  };

  const selectedStageName = selectedNode.plugin.label;

  const adjacencyMap = getAdjacencyMap(connections);
  const { tracers, previousStages } = getTracersAndPreviousStageInfo(
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
      const previewData = getRecords(res, selectedNode, previousStages);
      updatePreviewCb(previewData);
      loadingPreviewCb(false);
    },
    (err) => {
      loadingPreviewCb(false);

      if (errorCb) {
        errorCb(err);
      }
      // tslint:disable-next-line: no-console
      console.log('Error loading preview data: ', err);
    }
  );
}

export function getPreviousStageNames(selectedStage: string, adjMap: IAdjacencyMap) {
  const keys = adjMap.keySeq().toArray();
  const prevStageNames = keys.filter((key) => adjMap.get(key).includes(selectedStage));
  return prevStageNames;
}

function getTracersAndPreviousStageInfo(
  selectedStageName: string,
  adjacencyMap: IAdjacencyMap,
  stages: IStage[],
  connections: IConnection[]
) {
  let tracers: List<string> = List([selectedStageName]);
  const previousStageNames: string[] = getPreviousStageNames(selectedStageName, adjacencyMap);

  // source nodes have no previous stage
  if (isEmpty(previousStageNames)) {
    return { tracers: tracers.toArray() };
  }

  const previousStages: IPreviousStageInfo = {};
  previousStageNames.forEach((previousStageName) => {
    let previousStage = stages.find((stage) => stage.name === previousStageName);
    previousStages[previousStageName] = {};

    // TO DO [CDAP-16690]: Investigate whether we need the splitter logic here, since we aren't currently using it
    if (previousStage.plugin.type === 'splittertransform') {
      const previousStageConnection = connections.find(
        (connection) => connection.from === previousStageName && connection.to === selectedStageName
      );
      if (previousStageConnection) {
        previousStages[previousStageName].port = previousStageConnection.port;
      }
    } else {
      // If we have multiple condition nodes in a row, we have traverse back
      // until we find a node that actually has records out
      while (previousStage && previousStage.plugin.type === 'condition') {
        previousStages[previousStageName].condition = true;
        previousStageName = adjacencyMap
          .keySeq()
          .find((key) => adjacencyMap.get(key).indexOf(previousStageName) !== -1);
        previousStage = stages.find((stage) => stage.name === previousStageName);
      }
    }
    tracers = tracers.push(previousStageName);
  });

  return {
    tracers: tracers.toArray(),
    previousStages,
  };
}

function getRecords(res, selectedNode: INode, previousStages: IPreviousStageInfo) {
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
          (port) => port.split('.').pop() === objectQuery(previousStages, stageName, 'port')
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
