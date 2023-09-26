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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.spark.Constant.Spark.ArtifactFetcher;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeEnv;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeUtils;
import io.cdap.cdap.app.runtime.spark.distributed.SparkExecutionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import io.cdap.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import io.cdap.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.spark.SparkConfig;
import io.cdap.cdap.master.spi.environment.spark.SparkDriverWatcher;
import io.cdap.cdap.master.spi.environment.spark.SparkLocalizeResource;
import io.cdap.cdap.master.spi.environment.spark.SparkSubmitContext;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Master environment spark submitter.
 */
public class MasterEnvironmentSparkSubmitter extends AbstractSparkSubmitter {
  private static final Logger LOG = LoggerFactory.getLogger(MasterEnvironmentSparkSubmitter.class);
  private final SparkExecutionService sparkExecutionService;
  private final SparkRuntimeContext runtimeContext;
  private final MasterEnvironment masterEnv;
  private final CConfiguration cConf;
  private final Map<String, String> namespaceConfig;
  private SparkConfig sparkConfig;
  private List<LocalizeResource> resources;
  private SparkDriverWatcher sparkDriverWatcher;

  /**
   * Master environment spark submitter constructor.
   */
  public MasterEnvironmentSparkSubmitter(CConfiguration cConf, LocationFactory locationFactory, String hostname,
                                         SparkRuntimeContext runtimeContext, MasterEnvironment masterEnv,
                                         ProgramOptions options) {
    ProgramRunId programRunId = runtimeContext.getProgram().getId().run(runtimeContext.getRunId().getId());
    WorkflowProgramInfo workflowInfo = runtimeContext.getWorkflowInfo();
    BasicWorkflowToken workflowToken = workflowInfo == null ? null : workflowInfo.getWorkflowToken();
    this.sparkExecutionService = new SparkExecutionService(locationFactory, hostname, programRunId, workflowToken);
    this.runtimeContext = runtimeContext;
    this.masterEnv = masterEnv;
    this.cConf = cConf;
    this.namespaceConfig = SystemArguments.getNamespaceConfigs(options.getArguments().asMap());
  }

  @Override
  protected URI getJobFile() throws Exception {
    return generateOrGetSparkConfig().getSparkJobFile();
  }

  @Override
  protected Iterable<LocalizeResource> getFiles(List<LocalizeResource> localizeResources) {
    this.resources = Collections.unmodifiableList(new ArrayList<>(localizeResources));
    // files are localized through SparkSubmitContext on master environment. Hence returning empty list
    return Collections.emptyList();
  }

  @Override
  protected Iterable<LocalizeResource> getArchives(List<LocalizeResource> localizeResources) {
    this.resources = Collections.unmodifiableList(new ArrayList<>(localizeResources));
    // files are localized through SparkSubmitContext on master environment. Hence returning empty list
    return Collections.emptyList();
  }

  @Override
  protected Map<String, String> generateSubmitConf(Map<String, String> appConf) throws Exception {
    Map<String, String> config = new HashMap<>(appConf);
    config.put(SparkConfig.DRIVER_ENV_PREFIX + "CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    config.put("spark.executorEnv.CDAP_LOG_DIR", ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    config.put("spark.executorEnv.ARTIFACT_FECTHER_PORT", cConf.get(ArtifactFetcher.PORT));

    SparkConfig environmentConfig = generateOrGetSparkConfig();
    config.putAll(mergeSparkConfs(appConf, environmentConfig));
    return config;
  }

  @VisibleForTesting
  static Map<String, String> mergeSparkConfs(Map<String, String> appConf, SparkConfig envConf) {
    Map<String, String> config = new HashMap<>(appConf);
    config.putAll(envConf.getConfigs());
    if (envConf.getExtraJavaOpts() != null) {
      prependConfig(config, "spark.driver.extraJavaOptions", envConf.getExtraJavaOpts());
      prependConfig(config, "spark.executor.extraJavaOptions", envConf.getExtraJavaOpts());
    }
    return config;
  }

  private static void prependConfig(Map<String, String> config, String key, String value) {
    if (config.containsKey(key)) {
      config.put(key, value + " " + config.get(key));
      return;
    }
    config.put(key, value);
  }

  @Override
  protected void addMaster(Map<String, String> configs, ImmutableList.Builder<String> argBuilder) throws Exception {
    argBuilder.add("--master").add(generateOrGetSparkConfig().getMaster()).add("--deploy-mode").add("cluster");
  }

  @Override
  protected List<String> beforeSubmit() throws Exception {
    sparkExecutionService.startAndWait();
    InetSocketAddress socketAddress = sparkExecutionService.getBindAddress();
    // use ip instead of hostname, as some environments (like kubernetes) don't work properly with hostname
    String uri = String.format("http://%s:%d", socketAddress.getAddress().getHostAddress(), socketAddress.getPort());
    SparkRuntimeEnv.setProperty(SparkConfig.DRIVER_ENV_PREFIX + SparkRuntimeUtils.CDAP_SPARK_EXECUTION_SERVICE_URI,
                                uri);

    if (cConf.get(io.cdap.cdap.app.runtime.spark.Constant.Spark.ArtifactFetcher.PORT) != null) {
      String artifactFetcherUri =
        String.format("http://%s:%s", socketAddress.getAddress().getHostAddress(),
                      cConf.get(io.cdap.cdap.app.runtime.spark.Constant.Spark.ArtifactFetcher.PORT));
      SparkRuntimeEnv.setProperty(SparkConfig.DRIVER_ENV_PREFIX + "ARTIFACT_FECTHER_URI",
                                  artifactFetcherUri);
    }

    sparkDriverWatcher = generateOrGetSparkConfig().getSparkDriverWatcher();
    sparkDriverWatcher.initialize();

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
    try {
      sparkDriverWatcher.close();
    } catch (Exception e) {
      LOG.warn("Error while closing spark driver watcher thread.", e);
    }
  }

  @Override
  protected boolean waitForFinish() throws Exception {
    return sparkDriverWatcher.waitForFinish().get();
  }

  private SparkConfig generateOrGetSparkConfig() throws Exception {
    if (sparkConfig == null) {
      SparkSpecification spec = runtimeContext.getSparkSpecification();
      int driverCores = spec.getDriverResources().getVirtualCores();
      String driverCoresKey = "task.driver." + SystemArguments.CORES_KEY;
      String value = runtimeContext.getRuntimeArguments().get(driverCoresKey);
      if (value != null) {
        try {
         driverCores = Integer.parseInt(value);
        } catch (NumberFormatException e) {
          LOG.warn("Invalid {} value: {}",
              driverCoresKey, value);
        }
      }
      int executorCores = spec.getExecutorResources().getVirtualCores();
      String executorCoresKey = "task.executor." + SystemArguments.CORES_KEY;
      value = runtimeContext.getRuntimeArguments().get(executorCoresKey);
      if (value != null) {
        try {
          executorCores = Integer.parseInt(value);
        } catch (NumberFormatException e) {
          LOG.warn("Invalid {} value: {}",
              executorCoresKey, value);
        }
      }
      SparkSubmitContext context = new SparkSubmitContext(getLocalizeResources(resources), namespaceConfig,
                                                          driverCores,
                                                          executorCores);
      sparkConfig = masterEnv.generateSparkSubmitConfig(context);
    }
    return sparkConfig;
  }

  private Map<String, SparkLocalizeResource> getLocalizeResources(List<LocalizeResource> resources) {
    Map<String, SparkLocalizeResource> map = new HashMap<>();
    for (LocalizeResource resource : resources) {
      map.put(FilenameUtils.getName(resource.getURI().toString()),
              new SparkLocalizeResource(resource.getURI(), resource.isArchive()));
    }
    return map;
  }
}
