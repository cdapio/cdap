/*
 * Copyright © 2016 Cask Data, Inc.
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

const pipelineJson = {
  "name": "",
  "artifact": {
    "name": "cdap-data-pipeline",
    "version": "5.0.0-SNAPSHOT",
    "scope": "SYSTEM"
  },
  "config": {
    "resources": {
      "memoryMB": 1024,
      "virtualCores": 1
    },
    "driverResources": {
      "memoryMB": 1024,
      "virtualCores": 1
    },
    "schedule": "0 * * * *",
    "connections": [
      {
        "from": "File",
        "to": "Wrangler"
      },
      {
        "from": "Wrangler",
        "to": "ModelTrainer"
      }
    ],
    "comments": [],
    "postActions": [],
    "engine": "mapreduce",
    "stages": [
      {
        "name": "File",
        "plugin": {
          "name": "File",
          "type": "batchsource",
          "label": "File",
          "artifact": {
            "name": "core-plugins",
            "version": "2.0.0-SNAPSHOT",
            "scope": "SYSTEM"
          },
          "properties": {
            "schema": "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"offset\",\"type\":\"long\"},{\"name\":\"body\",\"type\":\"string\"}]}",
            "format": "text",
            "recursive": "false",
            "filenameOnly": "false",
            "ignoreNonExistingFolders": "false",
            "referenceName": "FileNode",
            "path": ""
          }
        },
        "type": "batchsource",
        "label": "File",
        "icon": "icon-file"
      },
      {
        "name": "Wrangler",
        "plugin": {
          "name": "Wrangler",
          "type": "transform",
          "label": "Wrangler",
          "artifact": {
            "name": "wrangler-transform",
            "version": "3.1.0-SNAPSHOT",
            "scope": "SYSTEM"
          },
          "properties": {
            "field": "*",
            "precondition": "false",
            "threshold": "1",
            "schema": "",
            "directives": ""
          }
        },
        "type": "transform",
        "label": "Wrangler",
        "icon": "icon-DataPreparation"
      },
      {
        "name": "ModelTrainer",
        "plugin": {
          "name": "ModelTrainer",
          "type": "sparksink",
          "label": "ModelTrainer",
          "artifact": {
            "name": "ml-plugins",
            "version": "1.0.0-SNAPSHOT",
            "scope": "SYSTEM"
          },
          "properties": {
            "targetIsCategorical": "false",
            "overwrite": "true",
            "experimentId": "",
            "modelId": "",
            "outcome": "",
            "algorithm": "",
            "predictionsDataset": ""
          }
        },
        "type": "sparksink",
        "label": "ModelTrainer",
        "icon": "fa-plug"
      }
    ],
    "properties": {},
    "processTimingEnabled": true,
    "stageLoggingEnabled": true,
    "maxConcurrentRuns": 1
  }
};
export default pipelineJson;
