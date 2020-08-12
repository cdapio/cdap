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

function getRandomArbitrary(min = 1, max = 10000) {
  return Math.floor(Math.random() * (max - min) + min);
}

const DEFAULT_GCP_SERVICEACCOUNT_PATH = Cypress.env('gcp_service_account_path');
const DEFAULT_GCP_PROJECTID = Cypress.env('gcp_projectid');

const DEFAULT_GCS_CONNECTION_NAME = `gcs_${getRandomArbitrary()}`;
const DEFAULT_GCS_FOLDER = 'cdap-gcp-ui-test';
const DEFAULT_GCS_FILE = 'purchase_bad.csv';

const DEFAULT_BIGQUERY_CONNECTION_NAME = `bigquery_${getRandomArbitrary()}`;
const DEFAULT_BIGQUERY_DATASET = 'cdap_gcp_ui_test';
const DEFAULT_BIGQUERY_TABLE = 'users';

const DEFAULT_SPANNER_INSTANCE = 'cdap-gcp-ui-test';
const DEFAULT_SPANNER_DATABASE = 'test';
const DEFAULT_SPANNER_TABLE = 'users';
const DEFAULT_SPANNER_CONNECTION_NAME = `spanner_${getRandomArbitrary()}`;

const RUNTIME_ARGS_DEPLOYED_SELECTOR = 'runtimeargs-deployed';
const RUNTIME_ARGS_KEY_SELECTOR = 'runtimeargs-key';
const RUNTIME_ARGS_VALUE_SELECTOR = 'runtimeargs-value';

export {
  DEFAULT_BIGQUERY_CONNECTION_NAME,
  DEFAULT_BIGQUERY_DATASET,
  DEFAULT_BIGQUERY_TABLE,
  DEFAULT_GCP_SERVICEACCOUNT_PATH,
  DEFAULT_GCP_PROJECTID,
  DEFAULT_GCS_CONNECTION_NAME,
  DEFAULT_GCS_FOLDER,
  DEFAULT_GCS_FILE,
  DEFAULT_SPANNER_INSTANCE,
  DEFAULT_SPANNER_DATABASE,
  DEFAULT_SPANNER_TABLE,
  DEFAULT_SPANNER_CONNECTION_NAME,
  RUNTIME_ARGS_DEPLOYED_SELECTOR,
  RUNTIME_ARGS_KEY_SELECTOR,
  RUNTIME_ARGS_VALUE_SELECTOR,
};
