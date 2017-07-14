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

package co.cask.cdap.etl.spark.streaming;

import co.cask.cdap.api.Admin;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.InstanceConflictException;
import co.cask.cdap.api.spark.JavaSparkExecutionContext;
import co.cask.cdap.etl.api.streaming.StreamingContext;
import co.cask.cdap.etl.common.AbstractStageContext;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.spec.StageSpec;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.tephra.TransactionFailureException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of StreamingContext for Spark.
 */
public class DefaultStreamingContext extends AbstractStageContext implements StreamingContext {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStreamingContext.class);
  private static final String EXTERNAL_DATASET_TYPE = "externalDataset";

  private final JavaSparkExecutionContext sec;
  private final JavaStreamingContext jsc;
  private final Admin admin;

  public DefaultStreamingContext(StageSpec stageSpec, JavaSparkExecutionContext sec, JavaStreamingContext jsc) {
    super(sec.getPluginContext(), sec.getServiceDiscoverer(), sec.getMetrics(), stageSpec, new BasicArguments(sec));
    this.sec = sec;
    this.jsc = jsc;
    this.admin = sec.getAdmin();
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
  public void registerLineage(final String referenceName) throws DatasetManagementException,
    TransactionFailureException {
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
        context.getDataset(referenceName);
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
}
