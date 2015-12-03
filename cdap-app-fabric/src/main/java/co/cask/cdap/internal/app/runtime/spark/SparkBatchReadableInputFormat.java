/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.spark;

import co.cask.cdap.api.data.batch.BatchReadable;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.internal.app.runtime.batch.dataset.AbstractBatchReadableInputFormat;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.Map;

/**
 * An {@link InputFormat} for {@link Spark} jobs that reads from {@link Dataset}.
 *
 * @param <KEY>   Type of key.
 * @param <VALUE> Type of value.
 */
public final class SparkBatchReadableInputFormat<KEY, VALUE> extends AbstractBatchReadableInputFormat<KEY, VALUE> {

  @Override
  protected BatchReadable<KEY, VALUE> createBatchReadable(TaskAttemptContext context, String datasetName,
                                                          Map<String, String> datasetArgs) {
    return SparkContextProvider.getSparkContext().getBatchReadable(datasetName, datasetArgs);
  }
}
