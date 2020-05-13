/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import io.cdap.cdap.api.Admin;
import io.cdap.cdap.api.Transactionals;
import io.cdap.cdap.api.TxRunnable;
import io.cdap.cdap.api.data.DatasetContext;
import io.cdap.cdap.api.dataset.Dataset;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.InstanceConflictException;
import io.cdap.cdap.api.spark.JavaSparkExecutionContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.streaming.StreamingContext;
import io.cdap.cdap.etl.common.AbstractStageContext;
import io.cdap.cdap.etl.common.BasicArguments;
import io.cdap.cdap.etl.common.PipelineRuntime;
import io.cdap.cdap.etl.proto.v2.spec.StageSpec;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Default implementation of StreamingContext for Spark.
 */
public class DefaultStreamingContext extends AbstractStageContext implements StreamingContext {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamingContext.class);
  private static final String EXTERNAL_DATASET_TYPE = "externalDataset";

  private final JavaSparkExecutionContext sec;
  private final JavaStreamingContext jsc;
  private final Admin admin;
  private final boolean isPreviewEnabled;

  public DefaultStreamingContext(StageSpec stageSpec, JavaSparkExecutionContext sec, JavaStreamingContext jsc) {
    super(new PipelineRuntime(sec.getNamespace(), sec.getApplicationSpecification().getName(),
                              sec.getLogicalStartTime(), new BasicArguments(sec), sec.getMetrics(),
                              sec.getPluginContext(), sec.getServiceDiscoverer(), sec, sec, sec), stageSpec);
    this.sec = sec;
    this.jsc = jsc;
    this.admin = sec.getAdmin();
    this.isPreviewEnabled = sec.getDataTracer(stageSpec.getName()).isEnabled();
  }

  @Override
  public boolean isPreviewEnabled() {
    return isPreviewEnabled;
  }

  @Override
  public JavaStreamingContext getSparkStreamingContext() {
    return jsc;
  }

  @Override
  public JavaSparkExecutionContext getSparkExecutionContext() {
    return sec;
  }

  @Override
  public void registerLineage(String referenceName)
    throws DatasetManagementException, TransactionFailureException {

    try {
      if (!admin.datasetExists(referenceName)) {
        admin.createDataset(referenceName, EXTERNAL_DATASET_TYPE, DatasetProperties.EMPTY);
      }
    } catch (InstanceConflictException ex) {
      // Might happen if there is executed in multiple drivers in parallel. A race condition exists between check
      // for dataset existence and creation.
      LOG.debug("Dataset with name {} already created. Hence not creating the external dataset.", referenceName);
    }

    Transactionals.execute(sec, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        // we cannot instantiate ExternalDataset here - it is in CDAP data-fabric,
        // and this code (the pipeline app) cannot depend on that. Thus, use reflection
        // to invoke a method on the dataset.
        Dataset ds = context.getDataset(referenceName);
        try {
          Class<? extends Dataset> dsClass = ds.getClass();
          Method method = dsClass.getMethod("recordRead");
          method.invoke(ds);
        } catch (NoSuchMethodException e) {
          LOG.warn("ExternalDataset '{}' does not have method 'recordRead()'. " +
                     "Can't register read-only lineage for this dataset", referenceName);
        }
      }
    }, DatasetManagementException.class);
  }

  @Override
  public void execute(TxRunnable runnable) throws TransactionFailureException {
    sec.execute(runnable);
  }

  @Override
  public void execute(int timeout, TxRunnable runnable) throws TransactionFailureException {
    sec.execute(timeout, runnable);
  }

  @Override
  public void record(List<FieldOperation> operations) {
    throw new UnsupportedOperationException("Field lineage recording is not supported. Please record lineage " +
                                              "in prepareRun() stage");
  }
}
