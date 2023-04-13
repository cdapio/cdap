/*
 * Copyright © 2017 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.submit;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.spark.SparkPackageUtils;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextConfig;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeEnv;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeUtils;
import io.cdap.cdap.app.runtime.spark.distributed.SparkExecutionService;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.runtimejob.LaunchMode;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.filesystem.LocationFactory;

/**
 * A {@link SparkSubmitter} to submit Spark job that runs on cluster.
 */
public class DistributedSparkSubmitter extends AbstractSparkSubmitter {

  private final Configuration hConf;
  private final String schedulerQueueName;
  private final SparkExecutionService sparkExecutionService;
  private final long tokenRenewalInterval;
  private final LaunchMode launchMode;

  public DistributedSparkSubmitter(Configuration hConf, LocationFactory locationFactory,
      String hostname, SparkRuntimeContext runtimeContext,
      @Nullable String schedulerQueueName,
      LaunchMode launchMode) {
    this.hConf = hConf;
    this.schedulerQueueName = schedulerQueueName;
    ProgramRunId programRunId = runtimeContext.getProgram().getId().run(runtimeContext.getRunId().getId());
    WorkflowProgramInfo workflowInfo = runtimeContext.getWorkflowInfo();
    BasicWorkflowToken workflowToken = workflowInfo == null ? null : workflowInfo.getWorkflowToken();
    this.sparkExecutionService = new SparkExecutionService(locationFactory, hostname, programRunId, workflowToken);

    Arguments systemArgs = runtimeContext.getProgramOptions().getArguments();
    this.tokenRenewalInterval = systemArgs.hasOption(SparkRuntimeContextConfig.CREDENTIALS_UPDATE_INTERVAL_MS)
      ? Long.parseLong(systemArgs.getOption(SparkRuntimeContextConfig.CREDENTIALS_UPDATE_INTERVAL_MS))
      : -1L;
    this.launchMode = launchMode;
  }

  @Override
  protected Map<String, String> generateSubmitConf() {
    Map<String, String> config = new HashMap<>();
    if (schedulerQueueName != null && !schedulerQueueName.isEmpty()) {
      config.put("spark.yarn.queue", schedulerQueueName);
    }
    if (tokenRenewalInterval > 0) {
      config.put("spark.yarn.token.renewal.interval", Long.toString(tokenRenewalInterval));
    }
    config.put("spark.yarn.appMasterEnv.CDAP_LOG_DIR",  ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    config.put("spark.executorEnv.CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);

    config.put("spark.yarn.security.tokens.hbase.enabled", "false");
    config.put("spark.yarn.security.tokens.hive.enabled", "false");

    // Make Spark UI runs on random port. By default, Spark UI runs on port 4040 and it will do a sequential search
    // of the next port if 4040 is already occupied. However, during the process, it unnecessarily logs big stacktrace
    // as WARN, which pollute the logs a lot if there are concurrent Spark job running (e.g. a fork in Workflow).
    config.put("spark.ui.port", "0");

    return config;
  }

  @Override
  protected void addMaster(Map<String, String> configs, ImmutableList.Builder<String> argBuilder) {
    argBuilder.add("--master").add("yarn")
      .add("--deploy-mode").add("cluster");
  }

  @Override
  protected List<String> beforeSubmit() {
    // Add all Hadoop configurations to the SparkRuntimeEnv, prefix with "spark.hadoop.". This is
    // how Spark YARN client get hold of Hadoop configurations if those configurations are not in classpath,
    // which is true in CM cluster due to private hadoop conf directory (SPARK-13441) and YARN-4727
    for (Map.Entry<String, String> entry : hConf) {
      SparkRuntimeEnv.setProperty("spark.hadoop." + entry.getKey(), hConf.get(entry.getKey()));
    }
    if (launchMode == LaunchMode.CLIENT) {
      // in client launch mode, DistributedSparkProgramRunner never ran to add variables
      // from spark-env.sh, so make sure they are added to the app master.
      for (Map.Entry<String, String> envEntry : SparkPackageUtils.getSparkEnv().entrySet()) {
        SparkRuntimeEnv.setProperty(
            "spark.yarn.appMasterEnv." + envEntry.getKey(), envEntry.getValue());
      }
    }

    sparkExecutionService.startAndWait();
    SparkRuntimeEnv.setProperty("spark.yarn.appMasterEnv." + SparkRuntimeUtils.CDAP_SPARK_EXECUTION_SERVICE_URI,
                                sparkExecutionService.getBaseURI().toString());
    return Collections.emptyList();
  }

  @Override
  protected void triggerShutdown(long timeout, TimeUnit timeoutTimeUnit) {
    // Just stop the execution service and block on that.
    // It will wait until the "completed" call from the Spark driver.
    sparkExecutionService.setShutdownWaitSeconds(timeoutTimeUnit.toSeconds(timeout));
    sparkExecutionService.stopAndWait();
  }

  @Override
  protected void onCompleted(boolean succeeded) {
    sparkExecutionService.shutdownNow();
  }
}
