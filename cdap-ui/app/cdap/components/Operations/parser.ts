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

interface IRawDashboardData {
  application: {
    name: string;
    version: string;
  };
  artifact: {
    name: string;
    version: string;
    scope: string;
  };
  namespace: string;
  program: string;
  run: string;
  start: number;
  running?: number;
  end?: number;
  startMethod: string;
  status: string;
  type: string; // program type
}

export enum PIPELINE_TYPE {
  replication = 'Replication',
  etl = 'ETL',
}

export interface IParsedOperationsData extends IRawDashboardData {
  pipelineType: PIPELINE_TYPE;
}

const PIPELINE_ARTIFACT_MAP = {
  'cdap-data-pipeline': PIPELINE_TYPE.etl,
  'cdap-data-streams': PIPELINE_TYPE.etl,
  'delta-app': PIPELINE_TYPE.replication,
};

export function parseOperationsResponse(rawData: IRawDashboardData[]): IParsedOperationsData[] {
  const output: IParsedOperationsData[] = [];

  rawData.forEach((row) => {
    // filter out programs underneath the workflow for batch pipeline
    if (row.artifact.name === 'cdap-data-pipeline' && row.type !== 'WORKFLOW') {
      return;
    }

    const pipelineType = PIPELINE_ARTIFACT_MAP[row.artifact.name];

    // filter out non pipeline
    if (!pipelineType) {
      return;
    }

    const outputRowData: IParsedOperationsData = {
      ...row,
      pipelineType,
    };

    output.push(outputRowData);
  });

  return output;
}
