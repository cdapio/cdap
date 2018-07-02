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

export default function getPipelineConfig({
  file,
  experimentId,
  modelId
}) {
  return {
    "artifact": {},
    "description": "",
    "name": "",
    "config": {
      "connections": [],
      "comments": [],
      "postActions": [],
      "properties": {},
      "processTimingEnabled": true,
      "stageLoggingEnabled": true,
      "stages": [
        {
          "name": "File",
          "plugin": {
            "name": "File",
            "type": "batchsource",
            "label": "File",
            "artifact": corePluginArtifact,
            "properties": {
              "path": srcPath,
              "schema": "{\"name\":\"fileRecord\",\"type\":\"record\",\"fields\":[{\"name\":\"offset\",\"type\":\"long\"},{\"name\":\"body\",\"type\":\"string\"}]}"
            }
          }
        },
        {
          "name": "Wrangler",
          "plugin": {
            "name": "Wrangler",
            "type": "transform",
            "label": "Wrangler",
            "artifact": wranglerArtifact,
            "properties": {
              "field": "*",
              "precondition": "false",
              "threshold": "1",
              "directives": directives
            }
          }
        },
        {
          "name": "MLPredictor",
          "plugin": {
              "name": "MLPredictor",
              "type": "sparkcompute",
              "label": "MLPredictor",
              "artifact": mmdsPluginsArtifact,
              "properties": {
                "experimentId": experimentId,
                "modelId": modelId
              }
          }
        }
      ],
      "schedule": "0 * * * *",
      "engine": "mapreduce",
      "numOfRecordsPreview": 100,
      "maxConcurrentRuns": 1
    }
  };
}
