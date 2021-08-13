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
<<<<<<< HEAD
import io.cdap.cdap.api.spark.Spark;
=======
>>>>>>> 11f1c2c4886 (wip - thin impl)
import io.cdap.cdap.app.runtime.Arguments;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextConfig;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeEnv;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeUtils;
import io.cdap.cdap.app.runtime.spark.distributed.SparkExecutionService;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
<<<<<<< HEAD
import io.cdap.cdap.master.spi.environment.SparkConfigs;
=======
>>>>>>> 11f1c2c4886 (wip - thin impl)
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

<<<<<<< HEAD
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
=======
>>>>>>> 11f1c2c4886 (wip - thin impl)
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * k8s spark submitter.
 */
public class KubeSparkSubmitter extends AbstractSparkSubmitter {
  private static final Logger LOG = LoggerFactory.getLogger(KubeSparkSubmitter.class);

  private final Configuration hConf;
  private final SparkExecutionService sparkExecutionService;
  private final SparkConfigs sparkConfigs;

  /**
   * kube spark submitter
   * @param hConf
   * @param locationFactory
   * @param hostname
   * @param runtimeContext
   * @param sparkConfigs
   */
  public KubeSparkSubmitter(Configuration hConf, LocationFactory locationFactory,
                            String hostname, SparkRuntimeContext runtimeContext, SparkConfigs sparkConfigs) {
    this.hConf = hConf;
    ProgramRunId programRunId = runtimeContext.getProgram().getId().run(runtimeContext.getRunId().getId());
    WorkflowProgramInfo workflowInfo = runtimeContext.getWorkflowInfo();
    BasicWorkflowToken workflowToken = workflowInfo == null ? null : workflowInfo.getWorkflowToken();
    this.sparkExecutionService = new SparkExecutionService(locationFactory, hostname, programRunId, workflowToken);
    this.sparkConfigs = sparkConfigs;
  }

  @Override
  protected Map<String, String> getSubmitConf() {
    System.err.println("### Getting submit configs from kube submitter");
    Map<String, String> config = new HashMap<>();
    config.put("spark.kubernetes.driverEnv.CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    config.put("spark.executorEnv.CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    config.putAll(sparkConfigs.getConfigs());
    File templateFile = new File("podTemplate");
    try (FileWriter writer = new FileWriter(templateFile)) {
      writer.write(sparkConfigs.getPodTemplateString());
      config.put("spark.kubernetes.driver.podTemplateFile", templateFile.getAbsolutePath());
    } catch (IOException e) {
      LOG.error("Error while writing file.", e);
    }


    return config;
  }

  @Override
  protected void addMaster(Map<String, String> configs, ImmutableList.Builder<String> argBuilder) {
    System.err.println("### Kube master address: " + "k8s://" + sparkConfigs.getMasterBasePath());
    argBuilder.add("--master").add("k8s://" + sparkConfigs.getMasterBasePath())
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

    sparkExecutionService.startAndWait();
    SparkRuntimeEnv.setProperty("spark.yarn.appMasterEnv." + SparkRuntimeUtils.CDAP_SPARK_EXECUTION_SERVICE_URI,
                                sparkExecutionService.getBaseURI().toString());
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
