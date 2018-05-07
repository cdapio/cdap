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

import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.app.guice.ClusterMode;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramClassLoaderProvider;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.spark.SparkPackageUtils;
import co.cask.cdap.app.runtime.spark.SparkProgramRuntimeProvider;
import co.cask.cdap.app.runtime.spark.SparkResourceFilters;
import co.cask.cdap.app.runtime.spark.SparkRuntimeContextConfig;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.FilterClassLoader;
import co.cask.cdap.common.twill.HadoopClassExcluder;
import co.cask.cdap.internal.app.runtime.batch.distributed.MapReduceContainerHelper;
import co.cask.cdap.internal.app.runtime.distributed.DistributedProgramRunner;
import co.cask.cdap.internal.app.runtime.distributed.LocalizeResource;
import co.cask.cdap.internal.app.runtime.distributed.ProgramLaunchConfig;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.runtime.spi.SparkCompat;
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
import org.apache.twill.api.TwillRunner;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A {@link ProgramRunner} for launching {@link Spark} program in distributed mode. It starts
 * a YARN application to act as the Spark client. A second YARN application will be launched
 * by Spark framework as the actual Spark program execution.
 */
public final class DistributedSparkProgramRunner extends DistributedProgramRunner
                                                 implements ProgramClassLoaderProvider {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedSparkProgramRunner.class);

  private final LocationFactory locationFactory;
  private final SparkCompat sparkCompat;

  @Inject
  @VisibleForTesting
  public DistributedSparkProgramRunner(SparkCompat sparkComat, CConfiguration cConf, YarnConfiguration hConf,
                                       Impersonator impersonator, LocationFactory locationFactory,
                                       ClusterMode clusterMode,
                                       @Constants.AppFabric.ProgramRunner TwillRunner twillRunner) {
    super(cConf, hConf, impersonator, clusterMode, twillRunner);
    this.sparkCompat = sparkComat;
    this.locationFactory = locationFactory;
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
  protected void validateOptions(Program program, ProgramOptions options) {
    super.validateOptions(program, options);

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
  }

  @Override
  protected void setupLaunchConfig(ProgramLaunchConfig launchConfig, Program program, ProgramOptions options,
                                   CConfiguration cConf, Configuration hConf, File tempDir) throws IOException {

    // Update the container hConf
    if (clusterMode == ClusterMode.ON_PREMISE) {
      // Kerberos is only supported in on premise mode
      hConf.set(Constants.Explore.HIVE_METASTORE_TOKEN_SIG, Constants.Explore.HIVE_METASTORE_TOKEN_SERVICE_NAME);

      if (SecurityUtil.isKerberosEnabled(cConf)) {
        // Need to divide the interval by 0.8 because Spark logic has a 0.8 discount on the interval
        // If we don't offset it, it will look for the new credentials too soon
        // Also add 5 seconds to the interval to give master time to push the changes to the Spark client container
        long interval = (long) ((TokenSecureStoreRenewer.calculateUpdateInterval(cConf, hConf) + 5000) / 0.8);
        launchConfig.addExtraSystemArgument(SparkRuntimeContextConfig.CREDENTIALS_UPDATE_INTERVAL_MS,
                                            Long.toString(interval));
      }
    }

    // Setup the launch config
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    SparkSpecification spec = appSpec.getSpark().get(program.getName());

    Map<String, String> clientArgs = RuntimeArguments.extractScope("task", "client",
                                                                   options.getUserArguments().asMap());
    // Add runnable. Only one instance for the spark client
    launchConfig.addRunnable(spec.getName(), new SparkTwillRunnable(spec.getName()), 1,
                             clientArgs, spec.getClientResources(), 0);

    Map<String, String> extraEnv = new HashMap<>();
    extraEnv.put(Constants.SPARK_COMPAT_ENV, sparkCompat.getCompat());

    if (clusterMode == ClusterMode.ON_PREMISE) {
      extraEnv.putAll(SparkPackageUtils.getSparkClientEnv());

      // Add extra resources, classpath, dependencies, env and setup ClassAcceptor
      Map<String, LocalizeResource> localizeResources = new HashMap<>();
      SparkPackageUtils.prepareSparkResources(sparkCompat, locationFactory, tempDir, localizeResources, extraEnv);

      // Add the mapreduce resources and path as well for the InputFormat/OutputFormat classes
      MapReduceContainerHelper.localizeFramework(hConf, localizeResources);

      launchConfig
        .addExtraResources(localizeResources)
        .addExtraClasspath(MapReduceContainerHelper.addMapReduceClassPath(hConf, new ArrayList<String>()));
    } else {
      // No need to rewrite YARN client
      cConf.setBoolean(Constants.AppFabric.SPARK_YARN_CLIENT_REWRITE, false);

      // For isolated mode, the hadoop classes comes from the hadoop classpath in the target cluster directly
      launchConfig.addExtraClasspath(Collections.singletonList("$HADOOP_CLASSPATH"));

      extraEnv.put(SparkPackageUtils.SPARK_YARN_MODE, "true");
    }

    launchConfig
      .addExtraEnv(extraEnv)
      .addExtraDependencies(SparkProgramRuntimeProvider.class)
      .addExtraSystemArgument(SparkRuntimeContextConfig.DISTRIBUTED_MODE, Boolean.TRUE.toString())
      .setClassAcceptor(createBundlerClassAcceptor());
  }


  private ClassAcceptor createBundlerClassAcceptor() throws MalformedURLException {
    final Set<URL> urls = new HashSet<>();
    for (File file : SparkPackageUtils.getLocalSparkLibrary(sparkCompat)) {
      urls.add(file.toURI().toURL());
    }

    return new HadoopClassExcluder() {
      @Override
      public boolean accept(String className, URL classUrl, URL classPathUrl) {
        // Exclude both hadoop and spark classes.
        if (urls.contains(classPathUrl)) {
          return false;
        }
        return super.accept(className, classUrl, classPathUrl)
          && !className.startsWith("org.apache.spark.");
      }
    };
  }

  @Override
  public ClassLoader createProgramClassLoaderParent() {
    return new FilterClassLoader(getClass().getClassLoader(), SparkResourceFilters.SPARK_PROGRAM_CLASS_LOADER_FILTER);
  }
}
