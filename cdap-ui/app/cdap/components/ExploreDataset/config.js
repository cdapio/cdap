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
export const RAF_ACCESS_TOKEN = "AhpkZWVwaWthLm1laHRhAOr1lLjcWurlx4rdWuaF+qACQMwUZZXczEPF//98fX+jkzY1tXFY1Lg6oBw+RDAskaSe";
export const IS_OFFLINE = false;
export const USE_REMOTE_SERVER = false;

export const PIPELINE_RUN_NAME = "pipelineRunName";
export const PIPELINE_SCHEMAS = "dataSchemaNames";

export const SUCCEEDED = "Succeeded";
export const DEPLOYED = "Deployed";
export const FAILED = "Failed";
export const RUNNING = "Running";
export const TOTAL = "Total";

export const PipeLineStatusConfig = [
  {
    name:'DEPLOYED',
    edit:true,
    clone:true,
    delete:true
  },
  {
    name:'DRAFT',
    edit:true,
    clone:true,
    delete:true
  },
  {
    name:'FAILED',
    edit:true,
    clone:true,
    delete:true
  },
  {
    name:'KILLED',
    edit:true,
    clone:true,
    delete:true
  },
  {
    name:'PENDING',
    edit:false,
    clone:false,
    delete:false
  },
  {
    name:'RESUMING',
    edit:false,
    clone:false,
    delete:false
  },
  {
    name:'RUNNING',
    edit:false,
    clone:false,
    delete:false
  },
  {
    name:'STARTING',
    edit:false,
    clone:false,
    delete:false
  },
  {
    name:'SUCCEDED',
    edit:true,
    clone:true,
    delete:true
  },
  {
    name:'SUSPENDED',
    edit:false,
    clone:false,
    delete:false
  },
  {
    name:'UNKNOWN',
    edit:true,
    clone:true,
    delete:true
  }
];

