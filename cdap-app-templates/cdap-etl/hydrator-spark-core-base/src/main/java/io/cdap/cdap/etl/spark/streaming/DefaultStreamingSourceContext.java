/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.spark.streaming;

import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.lineage.AccessType;
import io.cdap.cdap.etl.api.streaming.StreamingSourceContext;
import io.cdap.cdap.etl.batch.AbstractBatchContext;
import io.cdap.cdap.etl.common.ExternalDatasets;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Default implementation of streaming source context, this method will not start its own transaction when registering
 * lineage since the prepareRun() method is run in its own transaction
 */
public class DefaultStreamingSourceContext extends AbstractBatchContext implements StreamingSourceContext {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamingContext.class);
  private static final String EXTERNAL_DATASET_TYPE = "externalDataset";
  private final JavaSparkExecutionContext sec;

  public DefaultStreamingSourceContext(PipelineRuntime pipelineRuntime, StageSpec stageSpec,
                                       DatasetContext datasetContext, JavaSparkExecutionContext sec) {
    super(pipelineRuntime, stageSpec, datasetContext, sec.getAdmin());
    this.sec = sec;
  }

  @Override
  public void registerLineage(String referenceName,
                              @Nullable Schema schema) throws DatasetManagementException, TransactionFailureException {
    ExternalDatasets.registerLineage(admin, referenceName, AccessType.READ, schema, () -> getDataset(referenceName));
  }
}
