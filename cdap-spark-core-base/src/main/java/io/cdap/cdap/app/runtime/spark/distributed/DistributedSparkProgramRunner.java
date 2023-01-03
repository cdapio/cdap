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

package io.cdap.cdap.app.runtime.spark.distributed;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Injector;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.spark.Spark;
import io.cdap.cdap.api.spark.SparkSpecification;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramClassLoaderProvider;
import io.cdap.cdap.app.runtime.ProgramController;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunner;
import io.cdap.cdap.app.runtime.spark.SparkPackageUtils;
import io.cdap.cdap.app.runtime.spark.SparkProgramRuntimeProvider;
import io.cdap.cdap.app.runtime.spark.SparkResourceFilter;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextConfig;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.twill.ProgramRuntimeClassAcceptor;
import io.cdap.cdap.internal.app.runtime.batch.distributed.MapReduceContainerHelper;
import io.cdap.cdap.internal.app.runtime.distributed.DistributedProgramRunner;
import io.cdap.cdap.internal.app.runtime.distributed.LocalizeResource;
import io.cdap.cdap.internal.app.runtime.distributed.ProgramLaunchConfig;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.runtime.spi.SparkCompat;
import io.cdap.cdap.security.TokenSecureStoreRenewer;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ClassAcceptor;
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
                                       @Constants.AppFabric.ProgramRunner TwillRunner twillRunner,
                                       Injector injector) {
    super(cConf, hConf, impersonator, clusterMode, twillRunner, locationFactory);
    this.sparkCompat = sparkComat;
    this.locationFactory = locationFactory;
    if (!cConf.getBoolean(Constants.AppFabric.PROGRAM_REMOTE_RUNNER, false)) {
      this.namespaceQueryAdmin = injector.getInstance(NamespaceQueryAdmin.class);
    }
  }

  @Override
  public ProgramController createProgramController(ProgramRunId programRunId, TwillController twillController) {
    return new SparkTwillProgramController(programRunId, twillController).startListen();
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

      if (SecurityUtil.isKerberosEnabled(cConf)) {
        // Need to divide the interval by 0.8 because Spark logic has a 0.8 discount on the interval
        // If we don't offset it, it will look for the new credentials too soon
        // Also add 5 seconds to the interval to give master time to push the changes to the Spark client container
        long interval = (long) ((TokenSecureStoreRenewer.calculateUpdateInterval(hConf) + 5000) / 0.8);
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

    if (sparkCompat.getCompat().equals(SparkCompat.SPARK2_2_11.getCompat())) {
      // No need to rewrite YARN client
      cConf.setBoolean(Constants.AppFabric.SPARK_YARN_CLIENT_REWRITE, false);
    }

    // In isolated mode, we don't need to localize spark/mr framework from app-fabric.
    // We only need to setup localization to (yarn) container when this program runner
    // is running in the remote runtime process
    // Add extra resources, classpath, dependencies, env and setup ClassAcceptor
    if (clusterMode == ClusterMode.ON_PREMISE || cConf.getBoolean(Constants.AppFabric.PROGRAM_REMOTE_RUNNER, false)) {
      Map<String, LocalizeResource> localizeResources = new HashMap<>();
      SparkPackageUtils.prepareSparkResources(sparkCompat, locationFactory, tempDir, localizeResources, extraEnv);

      // Add the mapreduce resources and path as well for the InputFormat/OutputFormat classes
      MapReduceContainerHelper.localizeFramework(hConf, localizeResources);

      launchConfig
        .addExtraResources(localizeResources)
        .addExtraClasspath(MapReduceContainerHelper.addMapReduceClassPath(hConf, new ArrayList<String>()));
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

    return new ProgramRuntimeClassAcceptor() {
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
    return new FilterClassLoader(getClass().getClassLoader(), new SparkResourceFilter());
  }
}
