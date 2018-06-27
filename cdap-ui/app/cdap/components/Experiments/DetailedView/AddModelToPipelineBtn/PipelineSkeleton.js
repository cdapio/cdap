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
  mmdsPluginsArtifact,
  experimentId,
  modelId,
  predictionField
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
          "name": "MLPredictor",
          "plugin": {
              "name": "MLPredictor",
              "type": "sparkcompute",
              "label": "MLPredictor",
              "artifact": mmdsPluginsArtifact,
              "properties": {
                "experimentId": experimentId,
                "modelId": modelId,
                "predictionField": predictionField
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
