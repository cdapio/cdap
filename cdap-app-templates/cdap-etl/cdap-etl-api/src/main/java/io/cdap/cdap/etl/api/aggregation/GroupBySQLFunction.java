/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.etl.api.aggregation;

import io.cdap.cdap.etl.api.relational.SQLFunction;

/**
 * Aggregations supported by SQL Engines when generating group by definitions.
 */
public enum GroupBySQLFunction implements SQLFunction {
  AVG,
  COUNT,
  COUNT_DISTINCT,
  COUNT_NULLS,
  FIRST,
  LAST,
  MAX,
  MIN,
  STANDARD_DEVIATION,
  SUM,
  VARIANCE,
  ARRAY_AGG,
  ARRAY_AGG_DISTINCT,
  CONCAT,
  CONCAT_DISTINCT,
  LOGICAL_AND,
  LOGICAL_OR;
}
