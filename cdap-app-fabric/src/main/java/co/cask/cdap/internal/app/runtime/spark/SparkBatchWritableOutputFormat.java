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

import co.cask.cdap.api.data.batch.BatchWritable;
import co.cask.cdap.internal.app.runtime.batch.dataset.AbstractBatchWritableOutputFormat;
import co.cask.cdap.internal.app.runtime.batch.dataset.CloseableBatchWritable;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.util.Map;

/**
 * An {@link OutputFormat} for writing into {@link BatchWritable} from Spark.
 *
 * @param <KEY>   Type of key.
 * @param <VALUE> Type of value.
 */
public final class SparkBatchWritableOutputFormat<KEY, VALUE> extends AbstractBatchWritableOutputFormat<KEY, VALUE> {

  @Override
  protected CloseableBatchWritable<KEY, VALUE> createBatchWritable(TaskAttemptContext context,
                                                                   String datasetName,
                                                                   Map<String, String> datasetArgs) {
    return SparkContextProvider.getSparkContext().getBatchWritable(datasetName, datasetArgs);
  }
}
