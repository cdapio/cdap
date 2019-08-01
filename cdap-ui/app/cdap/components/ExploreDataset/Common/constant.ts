/*
 * Copyright © 2015-2017 Cask Data, Inc.
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

export const EDIT = "EDIT";
export const CLONE = "CLONE";
export const DELETE = "DELETE";

export const GET_PIPELINE = "GET_PIPELINE";
export const GET_SCHEMA = "GET_SCHEMA";
export const GET_PROPERTY = "GET_PROPERTY";
export const GET_CONFIGURATION = "GET_CONFIGURATION";
export const GET_SINKS = "GET_SINKS";
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
    GET_SINKS: "Error in getting sinks",
    SAVE_PIPELINE: "Error in saving pipeline",
    CREATE_PIPELINE: "Error in creating pipeline",
    READ_PIPELINE: "Error in getting pipeline configurations",
    EDIT_PIPELINE: "Error in  editing pipelines",
    DELETE_PIPELINE: "Error in deleting pipeline",
    GET_PIPE_LINE_DATA: "Error in getting pipeline data",
    GET_PIPE_LINE_FILTERED: "Error in getting filtered pipeline data",
    GET_FEATURE_CORRELAION: "Error in getting correlation",
  };
