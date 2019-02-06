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

export const PIPELINE_TYPES = ["All", "Feature Generation Pipeline", "Selected Feature Pipeline"];

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


export const IS_OFFLINE = false;
export const USE_REMOTE_SERVER = false;

export const PIPELINE_RUN_NAME = "pipelineRunName";
export const PIPELINE_SCHEMAS = "dataSchemaNames";

export const SUCCEEDED = "Succeeded";
export const DEPLOYED = "Deployed";
export const FAILED = "Failed";
export const RUNNING = "Running";
