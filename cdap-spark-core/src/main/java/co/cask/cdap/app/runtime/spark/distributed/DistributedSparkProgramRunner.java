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

package co.cask.cdap.app.runtime.spark.distributed;

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContextConfig;
import co.cask.cdap.app.runtime.spark.SparkRuntimeUtils;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.ProgramClassLoader;
import co.cask.cdap.common.lang.ProgramClassLoaderProvider;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.distributed.AbstractDistributedProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.security.TokenSecureStoreUpdater;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;

/**
 * A {@link ProgramRunner} for launching {@link Spark} program in distributed mode. It starts
 * a YARN application to act as the Spark client. A second YARN application will be launched
 * by Spark framework as the actual Spark program execution.
 */
public final class DistributedSparkProgramRunner extends AbstractDistributedProgramRunner
                                                 implements ProgramClassLoaderProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedSparkProgramRunner.class);

  @Inject
  DistributedSparkProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf, CConfiguration cConf,
                                TokenSecureStoreUpdater tokenSecureStoreUpdater) {
    super(twillRunner, createConfiguration(hConf), cConf, tokenSecureStoreUpdater);
  }

  @Override
  protected ProgramController launch(Program program, ProgramOptions options,
                                     Map<String, LocalizeResource> localizeResources,
                                     File tempDir, AbstractDistributedProgramRunner.ApplicationLauncher launcher) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification for %s", program.getId());

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type for %s", program.getId());
    Preconditions.checkArgument(processorType == ProgramType.SPARK,
                                "Only SPARK process type is supported. Program type is %s for %s",
                                processorType, program.getId());

    SparkSpecification spec = appSpec.getSpark().get(program.getName());
    Preconditions.checkNotNull(spec, "Missing SparkSpecification for %s", program.getId());

    // Localize the spark-assembly jar and spark conf zip
    String sparkAssemblyJarName = SparkUtils.prepareSparkResources(tempDir, localizeResources);

    LOG.info("Launching Spark program: {}", program.getId());
    TwillController controller = launcher.launch(
      new SparkTwillApplication(program, spec, localizeResources, eventHandler), sparkAssemblyJarName);

    RunId runId = RunIds.fromString(options.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    return new SparkTwillProgramController(program.getId().toEntityId(), controller, runId).startListen();
  }

  private static YarnConfiguration createConfiguration(YarnConfiguration hConf) {
    YarnConfiguration configuration = new YarnConfiguration(hConf);
    configuration.setBoolean(SparkRuntimeContextConfig.HCONF_ATTR_CLUSTER_MODE, true);
    return configuration;
  }

  @Override
  public ProgramClassLoader createProgramClassLoader(CConfiguration cConf, File dir) {
    return SparkRuntimeUtils.createProgramClassLoader(cConf, dir, getClass().getClassLoader());
  }
}
