/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramClassLoaderProvider;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.spark.SparkPackageUtils;
import co.cask.cdap.app.runtime.spark.SparkProgramRuntimeProvider;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContextConfig;
import co.cask.cdap.app.runtime.spark.SparkRuntimeUtils;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.twill.HadoopClassExcluder;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.distributed.DistributedProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.security.impersonation.SecurityUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link ProgramRunner} for launching {@link Spark} program in distributed mode. It starts
 * a YARN application to act as the Spark client. A second YARN application will be launched
 * by Spark framework as the actual Spark program execution.
 */
public final class DistributedSparkProgramRunner extends DistributedProgramRunner
                                                 implements ProgramClassLoaderProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedSparkProgramRunner.class);

  @Inject
  @VisibleForTesting
  public DistributedSparkProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf, CConfiguration cConf,
                                       TokenSecureStoreRenewer tokenSecureStoreRenewer,
                                       Impersonator impersonator) {
    super(twillRunner, hConf, cConf, tokenSecureStoreRenewer, impersonator);
  }

  @Override
  public ProgramController createProgramController(TwillController twillController,
                                                   ProgramDescriptor programDescriptor, RunId runId) {
    return createProgramController(twillController, programDescriptor.getProgramId(), runId);
  }

  private ProgramController createProgramController(TwillController twillController, ProgramId programId, RunId runId) {
    return new SparkTwillProgramController(programId, twillController, runId).startListen();
  }

  @Override
  protected ClassAcceptor getBundlerClassAcceptor(Program program) {
    final File sparkAssemblyJar = SparkPackageUtils.locateSparkAssemblyJar();
    return new HadoopClassExcluder() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        // Exclude both hadoop and spark classes.
        if (sparkAssemblyJar != null && sparkAssemblyJar.equals(classPathUrl)) {
          return false;
        }
        return super.accept(className, classUrl, classPathUrl)
          && !className.startsWith("org.apache.spark.");
      }
    };
  }

  @Override
  protected Map<String, ProgramTwillApplication.RunnableResource> getRunnables(Program program,
                                                                               ProgramOptions programOptions) {
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

    Map<String, String> clientArgs = RuntimeArguments.extractScope("task", "client",
                                                                   programOptions.getUserArguments().asMap());
    Resources resources = SystemArguments.getResources(clientArgs, spec.getClientResources());

    Map<String, ProgramTwillApplication.RunnableResource> runnables = new HashMap<>();
    runnables.put(spec.getName(), new ProgramTwillApplication.RunnableResource(new SparkTwillRunnable(spec.getName()),
                                                                               createResourceSpec(resources, 1)));
    return runnables;
  }

  @Override
  protected Map<String, LocalizeResource> getExtraLocalizeResources(Program program, File tempDir) {
    Map<String, LocalizeResource> localizeResources = new HashMap<>();
    SparkPackageUtils.prepareSparkResources(tempDir, localizeResources);
    return localizeResources;
  }

  @Override
  protected void prepareLaunch(Program program, TwillPreparer preparer) {
    preparer
      .withClassPaths(SparkPackageUtils.locateSparkAssemblyJar().getName())
      .withDependencies(SparkProgramRuntimeProvider.class)
      .withEnv(SparkPackageUtils.getSparkClientEnv());
  }

  @Override
  protected Configuration createContainerHConf(Program program, Configuration hConf) {
    Configuration result = super.createContainerHConf(program, hConf);
    result.setBoolean(SparkRuntimeContextConfig.HCONF_ATTR_CLUSTER_MODE, true);

    if (SecurityUtil.isKerberosEnabled(cConf)) {
      // Need to divide the interval by 0.8 because Spark logic has a 0.8 discount on the interval
      // If we don't offset it, it will look for the new credentials too soon
      // Also add 5 seconds to the interval to give master time to push the changes to the Spark client container
      result.setLong(SparkRuntimeContextConfig.HCONF_ATTR_CREDENTIALS_UPDATE_INTERVAL_MS,
                            (long) ((secureStoreRenewer.getUpdateInterval() + 5000) / 0.8));
    }
    return result;
  }

  @Override
  public ClassLoader createProgramClassLoaderParent() {
    return new FilterClassLoader(getClass().getClassLoader(), SparkRuntimeUtils.SPARK_PROGRAM_CLASS_LOADER_FILTER);
  }
}
