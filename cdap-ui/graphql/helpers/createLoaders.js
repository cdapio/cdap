/*
 * Copyright © 2019 Cask Data, Inc.
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

const DataLoader = require('dataloader');
const { batchProgramRuns } = require('./BatchEndpoints/programRuns');
const { batchTotalRuns } = require('./BatchEndpoints/totalRuns');

function createLoaders(auth) {
  return {
    programRuns: new DataLoader((req) => batchProgramRuns(req, auth), { cache: false }),
    totalRuns: new DataLoader((req) => batchTotalRuns(req, auth), { cache: false }),
  };
}

module.exports = {
  createLoaders,
};
