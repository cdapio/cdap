/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.engine.sql;

import io.cdap.cdap.etl.api.relational.Capability;

/**
 * Defines capabilities for SQL Engine factories.
 */
@SuppressWarnings({"abbreviationAsWordInName"})
public enum StandardSQLCapabilities implements Capability {
  /**
   * Defines that factory implements SQL92 language.
   */
  SQL92,
  /**
   * Defines that factory implements support for BigQuery specific language.
   */
  BIGQUERY,
  /**
   * Defines that factory implements support for Postgres SQL dialect.
   */
  POSTGRES,
  /**
   * Defines that factory implements support for Spark specific language
   */
  SPARK
}
