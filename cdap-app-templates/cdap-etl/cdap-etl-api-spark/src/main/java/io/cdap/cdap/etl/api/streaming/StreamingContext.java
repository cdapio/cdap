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

package co.cask.cdap.etl.api.streaming;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.StageContext;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.tephra.TransactionFailureException;

/**
 * Context for streaming plugin stages.
 */
@Beta
public interface StreamingContext extends StageContext, Transactional {

  /**
   * @return Spark JavaStreamingContext for the pipeline.
   */
  JavaStreamingContext getSparkStreamingContext();

  /**
   * @return CDAP JavaSparkExecutionContext for the pipeline.
   */
  JavaSparkExecutionContext getSparkExecutionContext();

  /**
   * Register lineage for this Spark program using the given reference name
   * @param referenceName reference name used for source
   * @throws DatasetManagementException thrown if there was an error in creating reference dataset
   * @throws TransactionFailureException thrown if there was an error while fetching the dataset to register usage
   */
  void registerLineage(String referenceName) throws DatasetManagementException, TransactionFailureException;
}
