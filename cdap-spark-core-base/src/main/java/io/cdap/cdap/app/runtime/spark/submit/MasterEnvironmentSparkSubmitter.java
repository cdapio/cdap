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

package io.cdap.cdap.app.runtime.spark.submit;

import com.google.common.collect.ImmutableList;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeEnv;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeUtils;
import io.cdap.cdap.app.runtime.spark.distributed.SparkExecutionService;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.master.spi.environment.spark.SparkConfig;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.filesystem.LocationFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Master environment spark submitter.
 */
public class MasterEnvironmentSparkSubmitter extends AbstractSparkSubmitter {
  private final SparkExecutionService sparkExecutionService;
  private final SparkConfig sparkConfig;

  /**
   * Master environment spark submitter constructor.
   */
  public MasterEnvironmentSparkSubmitter(LocationFactory locationFactory, String hostname,
                                         SparkRuntimeContext runtimeContext, SparkConfig sparkConfig) {
    ProgramRunId programRunId = runtimeContext.getProgram().getId().run(runtimeContext.getRunId().getId());
    WorkflowProgramInfo workflowInfo = runtimeContext.getWorkflowInfo();
    BasicWorkflowToken workflowToken = workflowInfo == null ? null : workflowInfo.getWorkflowToken();
    this.sparkExecutionService = new SparkExecutionService(locationFactory, hostname, programRunId, workflowToken);
    this.sparkConfig = sparkConfig;
  }

  @Override
  protected Map<String, String> getSubmitConf() {
    Map<String, String> config = new HashMap<>();
    config.put(SparkConfig.DRIVER_ENV_PREFIX + "CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    config.put("spark.executorEnv.CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    config.putAll(sparkConfig.getConfigs());
    return config;
  }

  @Override
  protected void addMaster(Map<String, String> configs, ImmutableList.Builder<String> argBuilder) {
    argBuilder.add("--master").add(sparkConfig.getMaster()).add("--deploy-mode").add("cluster");
  }

  @Override
  protected List<String> beforeSubmit() {
    sparkExecutionService.startAndWait();
    InetSocketAddress socketAddress = sparkExecutionService.getBindAddress();
    // use ip instead of hostname, as some environments (like kubernetes) don't work properly with hostname
    String uri = String.format("http://%s:%d", socketAddress.getAddress().getHostAddress(), socketAddress.getPort());
    SparkRuntimeEnv.setProperty(SparkConfig.DRIVER_ENV_PREFIX + SparkRuntimeUtils.CDAP_SPARK_EXECUTION_SERVICE_URI,
                                uri);
    return Collections.emptyList();
  }

  @Override
  protected void triggerShutdown() {
    // Just stop the execution service and block on that.
    // It will wait until the "completed" call from the Spark driver.
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
