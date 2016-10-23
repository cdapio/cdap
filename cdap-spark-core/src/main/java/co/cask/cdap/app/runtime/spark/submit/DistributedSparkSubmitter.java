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

package co.cask.cdap.app.runtime.spark.submit;

import co.cask.cdap.app.runtime.spark.SparkMainWrapper;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContext;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContextConfig;
import co.cask.cdap.app.runtime.spark.SparkRuntimeEnv;
import co.cask.cdap.app.runtime.spark.distributed.SparkExecutionService;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.filesystem.LocationFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link SparkSubmitter} to submit Spark job that runs on cluster.
 */
public class DistributedSparkSubmitter extends AbstractSparkSubmitter {

  private final Configuration hConf;
  private final String schedulerQueueName;
  private final SparkExecutionService sparkExecutionService;

  public DistributedSparkSubmitter(Configuration hConf, LocationFactory locationFactory,
                                   String hostname, SparkRuntimeContext runtimeContext,
                                   @Nullable String schedulerQueueName) {
    this.hConf = hConf;
    this.schedulerQueueName = schedulerQueueName;
    ProgramRunId programRunId = runtimeContext.getProgram().getId().toEntityId().run(runtimeContext.getRunId().getId());
    WorkflowProgramInfo workflowInfo = runtimeContext.getWorkflowInfo();
    BasicWorkflowToken workflowToken = workflowInfo == null ? null : workflowInfo.getWorkflowToken();
    this.sparkExecutionService = new SparkExecutionService(locationFactory, hostname, programRunId, workflowToken);
  }

  @Override
  protected Map<String, String> getSubmitConf() {
    Map<String, String> config = new HashMap<>();
    if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
      config.put("spark.yarn.queue", schedulerQueueName);
    }
    long updateInterval = hConf.getLong(SparkRuntimeContextConfig.HCONF_ATTR_CREDENTIALS_UPDATE_INTERVAL_MS, -1L);
    if (updateInterval > 0) {
      config.put("spark.yarn.token.renewal.interval", Long.toString(updateInterval));
    }
    config.put("spark.yarn.appMasterEnv.CDAP_LOG_DIR",  ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    config.put("spark.executorEnv.CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);

    return config;
  }

  @Override
  protected String getMaster(Map<String, String> configs) {
    return "yarn-cluster";
  }

  @Override
  protected List<String> beforeSubmit() {
    // Add all Hadoop configurations to the SparkRuntimeEnv, prefix with "spark.hadoop.". This is
    // how Spark YARN client get hold of Hadoop configurations if those configurations are not in classpath,
    // which is true in CM cluster due to private hadoop conf directory (SPARK-13441) and YARN-4727
    for (Map.Entry<String, String> entry : hConf) {
      SparkRuntimeEnv.setProperty("spark.hadoop." + entry.getKey(), hConf.get(entry.getKey()));
    }

    sparkExecutionService.startAndWait();
    return Collections.singletonList("--" + SparkMainWrapper.ARG_EXECUTION_SERVICE_URI()
                                       + "=" + sparkExecutionService.getBaseURI());
  }

  @Override
  protected void triggerShutdown() {
    // Just stop the execution service and block on that.
    // It will wait till the "completed" call from the Spark driver.
    sparkExecutionService.stopAndWait();
  }

  @Override
  protected void onCompleted(boolean succeeded) {
    if (succeeded) {
      sparkExecutionService.stopAndWait();
    } else {
      sparkExecutionService.shutdownNow();
    }
  }
}
