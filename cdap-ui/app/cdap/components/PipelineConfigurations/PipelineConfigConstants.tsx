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

const SPARK_EXECUTOR_INSTANCES = 'system.spark.spark.executor.instances';
const DEPRECATED_SPARK_MASTER = 'system.spark.spark.master';
const SPARK_BACKPRESSURE_ENABLED = 'system.spark.spark.streaming.backpressure.enabled';

const ENGINE_OPTIONS = {
  MAPREDUCE: 'mapreduce',
  SPARK: 'spark',
};

export {
  SPARK_EXECUTOR_INSTANCES,
  DEPRECATED_SPARK_MASTER,
  SPARK_BACKPRESSURE_ENABLED,
  ENGINE_OPTIONS,
};
