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

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.etl.api.engine.sql.dataset.SQLPushDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SparkPullDataset;
import io.cdap.cdap.etl.api.engine.sql.dataset.SparkPushDataset;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPullRequest;
import io.cdap.cdap.etl.api.engine.sql.request.SQLPushRequest;
import org.apache.spark.api.java.JavaSparkContext;

import javax.annotation.Nullable;

/**
 * Instance of a {@link SQLEngine} whih exposes methods to read table records using a Spark function.
 *
 * @param <KEY_OUT>   The type for the Output Key when mapping a StructuredRecord
 * @param <VALUE_OUT> The type for the Output Value when mapping a StructuredRecord
 * @param <KEY_IN>    The type for the Input Key when building a StructuredRecord
 * @param <VALUE_IN>  The type for the Input Value when building a StructuredRecord
 */
public interface SparkSQLEngine<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT>
  extends SQLEngine<KEY_IN, VALUE_IN, KEY_OUT, VALUE_OUT> {

  /**
   * Creates a Spark push datasets that can be used to push records into the dataset specified in the push request.
   * @param pushRequest the request containing information about the dataset name and schema.
   * @return {@link SQLPushDataset} instance that can be used to write records to the SQL Engine.
   */
  @Nullable
  SparkPushDataset<StructuredRecord> getSparkPushProvider(SQLPushRequest pushRequest);

  /**
   * Creates a Spark pull datasets that can be used to pull records from the dataset specified in the pull request.
   * @param pullRequest the request containing information about the dataset name and schema.
   * @return {@link SparkPullDataset} instance that can be used to read records from the SQL engine.
   */
  @Nullable
  SparkPullDataset<StructuredRecord> getSparkPullProvider(SQLPullRequest pullRequest);

}
