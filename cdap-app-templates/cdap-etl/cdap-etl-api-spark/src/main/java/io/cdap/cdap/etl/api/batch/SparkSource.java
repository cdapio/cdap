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

package io.cdap.cdap.etl.api.batch;

import io.cdap.cdap.api.annotation.Beta;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;


/**
 * SparkSource defines an initial stage of a Batch ETL Pipeline. In addition to configuring the Batch run, it
 * will create a new RDD for the rest of the pipeline to consume.
 *
 * {@link SparkSource#create} method is called inside the Batch Run while {@link SparkSource#prepareRun} and
 * {@link SparkSource#onRunFinish} methods are called on the client side, which launches the Batch run, before the
 * Batch run starts and after it finishes respectively.
 *
 * @param <OUT> The type of output record to the SparkSink.
 */
@Beta
public abstract class SparkSource<OUT> extends BatchConfigurable<SparkPluginContext> implements Serializable {

  public static final String PLUGIN_TYPE = "sparksource";

  private static final long serialVersionUID = 4829903051232692690L;

  /**
   * User Spark job which will be executed and is responsible for generating a new RDD for the resf of the pipeline
   * to consume
   *
   * @param context {@link SparkExecutionPluginContext} for this job
   */
  public abstract JavaRDD<OUT> create(SparkExecutionPluginContext context) throws Exception;

}
