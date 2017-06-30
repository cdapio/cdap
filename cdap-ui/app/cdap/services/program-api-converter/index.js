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

const apiProgramTypeConvert = {
  flow: 'flows',
  flows: 'flows',
  mapreduce: 'mapreduce',
  service: 'services',
  services: 'services',
  spark: 'spark',
  worker: 'workers',
  workers: 'workers',
  workflow: 'workflows',
  workflows: 'workflows'
};

const metricApiProgramTypeConvert = Object.assign({}, apiProgramTypeConvert, {
  workflows: 'workflow',
  workflow: 'workflow'
});
export function convertProgramToApi(program) {
  return apiProgramTypeConvert[program.toLowerCase()];
}

export function convertProgramToMetricParams(program) {
  return metricApiProgramTypeConvert[program.toLowerCase()];
}

