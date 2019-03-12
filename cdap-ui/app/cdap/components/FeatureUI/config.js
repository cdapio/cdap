import { getEpochDateString } from "./GridFormatters";

/*
 * Copyright © 2018 Cask Data, Inc.
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
export const REMOTE_IP = "http://192.168.156.36:11015";
const SERVICE_PORT = "11015";

export const SERVER_IP = (window.location.hostname == "localhost") ? REMOTE_IP : (window.location.protocol + "//" + window.location.hostname + ":" + SERVICE_PORT);
export const GET_PIPE_LINE_CORRELATED_DATA = "/v3/namespaces/default/apps/FeatureEngineeringApp/services/ManualFeatureSelectionService/methods/featureengineering/features/correlation/matrix/get?pipelineName=";

export const FEATURE_GENERATED_PIPELINE = "Feature Generation Pipeline";
export const FEATURE_SELECTED_PIPELINE = "Selected Feature Pipeline";

export const PIPELINE_TYPES = ["All", FEATURE_GENERATED_PIPELINE, FEATURE_SELECTED_PIPELINE];

export const GET_PIPELINE = "GET_PIPELINE";
export const GET_SCHEMA = "GET_SCHEMA";
export const GET_PROPERTY = "GET_PROPERTY";
export const GET_CONFIGURATION = "GET_CONFIGURATION";
export const SAVE_PIPELINE = "SAVE_PIPELINE";
export const CREATE_PIPELINE = "CREATE_PIPELINE";
export const CLONE_PIPELINE = "CLONE_PIPELINE";
export const READ_PIPELINE = "READ_PIPELINE";
export const EDIT_PIPELINE = "EDIT_PIPELINE";
export const DELETE_PIPELINE = "DELETE_PIPELINE";
export const GET_PIPE_LINE_DATA = "GET_PIPE_LINE_DATA";
export const GET_PIPE_LINE_FILTERED = "GET_PIPE_LINE_FILTERED";
export const GET_FEATURE_CORRELAION = "GET_FEATURE_CORRELAION";

export const ERROR_MESSAGES = {
  GET_PIPELINE: "Error in getting pipelines",
  GET_SCHEMA: "Error in getting dataset schemas",
  GET_PROPERTY: "Error in getting metadata configurations",
  GET_CONFIGURATION: "Error in getting engine configurations",
  SAVE_PIPELINE: "Error in saving pipeline",
  CREATE_PIPELINE: "Error in creating pipeline",
  READ_PIPELINE: "Error in getting pipeline configurations",
  EDIT_PIPELINE: "Error in  editing pipelines",
  DELETE_PIPELINE: "Error in deleting pipeline",
  GET_PIPE_LINE_DATA: "Error in getting pipeline data",
  GET_PIPE_LINE_FILTERED: "Error in getting filtered pipeline data",
  GET_FEATURE_CORRELAION: "Error in getting correlation"
};


export const IS_OFFLINE = false;
export const USE_REMOTE_SERVER = false;

export const PIPELINE_RUN_NAME = "pipelineRunName";
export const PIPELINE_SCHEMAS = "dataSchemaNames";

export const SUCCEEDED = "Succeeded";
export const DEPLOYED = "Deployed";
export const FAILED = "Failed";
export const RUNNING = "Running";
export const TOTAL = "Total";

export const FEATURE_GENERATED = "Feature Generated";
export const FEATURE_SELECTED = "Feature Selected";
export const AFEGridColumns = [
  {
    headerName: "Pipeline",
    field: "pipelineName",
    tooltipField: 'pipelineName',
    cellRenderer: 'feLinkRenderer',
    width: 400
  },
  {
    headerName: "Status",
    field: "status",
    tooltipField: 'status',
    cellRenderer: 'statusRenderer',
    width: 300,
  },
  {
    headerName: "Last Run Time",
    field: "lastStartEpochTime",
    tooltipField: 'lastStartEpochTime',
    valueFormatter: function(params) { return getEpochDateString(params); },
    suppressMenu: true,
    width: 300,
    filter: false
  },
  {
    headerName: "Type",
    field: "pipelineType",
    tooltipField: 'pipelineType',
    width: 250
  },
  {
    headerName: "",
    field: "pipelineName",
    width: 230,
    cellRenderer: 'fsLinkRenderer',
    suppressMenu: true,
    filter: false
  },
  {
    headerName: "",
    field: "pipelineName",
    width: 40,
    cellRenderer: 'cloneRenderer',
    suppressMenu: true,
  },
  {
    headerName: "",
    field: "pipelineName",
    width: 40,
    cellRenderer: 'editRenderer',
    suppressMenu: true,
  },
  {
    headerName: "",
    field: "pipelineName",
    width: 40,
    cellRenderer: 'deleteRenderer',
    suppressMenu: true,
  }
];
