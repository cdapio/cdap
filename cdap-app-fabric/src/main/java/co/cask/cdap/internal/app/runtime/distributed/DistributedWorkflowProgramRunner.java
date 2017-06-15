/*
 * Copyright © 2014-2017 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.distributed;

import co.cask.cdap.api.Resources;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRuntimeProvider;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.internal.app.runtime.ProgramRunners;
import co.cask.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.batch.distributed.MapReduceContainerHelper;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.Impersonator;
import co.cask.cdap.security.store.SecureStoreUtils;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.mapred.YarnClientProtocolProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A {@link ProgramRunner} to start a {@link Workflow} program in distributed mode.
 */
public final class DistributedWorkflowProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedWorkflowProgramRunner.class);

  private static final String HCONF_ATTR_CLUSTER_MODE = "cdap.spark.cluster.mode";
  private static final String HCONF_ATTR_CREDENTIALS_UPDATE_INTERVAL_MS = "cdap.spark.credentials.update.interval.ms";

  private final ProgramRuntimeProviderLoader runtimeProviderLoader;

  @Inject
  DistributedWorkflowProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf, CConfiguration cConf,
                                   TokenSecureStoreRenewer tokenSecureStoreRenewer,
                                   ProgramRuntimeProviderLoader runtimeProviderLoader,
                                   Impersonator impersonator) {
    super(twillRunner, createConfiguration(hConf, cConf, tokenSecureStoreRenewer), cConf, tokenSecureStoreRenewer,
          impersonator);
    this.runtimeProviderLoader = runtimeProviderLoader;
  }

  @Override
  protected void validateOptions(Program program, ProgramOptions options) {
    super.validateOptions(program, options);
    WorkflowSpecification spec = program.getApplicationSpecification().getWorkflows().get(program.getName());
    for (WorkflowNode node : spec.getNodes()) {
      if (node.getType().equals(WorkflowNodeType.ACTION)) {
        SystemArguments.validateTransactionTimeout(options.getUserArguments().asMap(),
                                                   cConf, "action", node.getNodeId());
      }
    }
  }

  @Override
  public ProgramController createProgramController(TwillController twillController,
                                                   ProgramDescriptor programDescriptor, RunId runId) {
    return createProgramController(twillController, programDescriptor.getProgramId(), runId);
  }

  private ProgramController createProgramController(TwillController twillController, ProgramId programId, RunId runId) {
    return new WorkflowTwillProgramController(programId, twillController, runId).startListen();
  }

  @Override
  protected ProgramController launch(Program program, ProgramOptions options,
                                     Map<String, LocalizeResource> localizeResources,
                                     File tempDir, ApplicationLauncher launcher) {
    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.WORKFLOW, "Only WORKFLOW process type is supported.");

    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(program.getName());
    Preconditions.checkNotNull(workflowSpec, "Missing WorkflowSpecification for %s", program.getName());

    // It the workflow has Spark, localize the spark-assembly jar
    List<String> extraClassPaths = new ArrayList<>();
    List<Class<?>> extraDependencies = new ArrayList<>();
    Map<String, String> environment = Collections.emptyMap();

    // Adds the extra classes that MapReduce needs
    extraDependencies.add(YarnClientProtocolProvider.class);

    // Add cdap-kms jars if kms is enabled and supported
    if (SecureStoreUtils.isKMSBacked(cConf) && SecureStoreUtils.isKMSCapable()) {
      extraDependencies.add(SecureStoreUtils.getKMSSecureStore());
    }

    // See if the Workflow has Spark or MapReduce in it
    DriverMeta driverMeta = findDriverResources(program.getApplicationSpecification().getSpark(),
                                                program.getApplicationSpecification().getMapReduce(), workflowSpec);

    // Due to caching in CDAP-7021, we need to always include Spark if it is available, regardless of the
    // Workflow contains Spark inside or not.
    // Adds the extra class that Spark runtime needed
    ProgramRuntimeProvider provider = runtimeProviderLoader.get(ProgramType.SPARK);
    if (provider != null) {
      try {
        String sparkAssemblyJarName = SparkUtils.prepareSparkResources(tempDir, localizeResources);
        // Localize the spark-assembly jar and spark conf zip
        extraClassPaths.add(sparkAssemblyJarName);
        extraDependencies.add(provider.getClass());
        environment = SparkUtils.getSparkClientEnv();
      } catch (Exception e) {
        if (driverMeta.hasSpark) {
          // If the Workflow actually has spark, we can't ignore this error.
          throw e;
        }
        // Otherwise, this can be ignore.
        LOG.debug("Spark assembly jar is not present. It doesn't affected Workflow {} since it doesn't use Spark.",
                  program.getId(), e);
      }

    } else if (driverMeta.hasSpark) {
      // If the workflow contains spark and yet the spark runtime provider is missing, then it's an error.
      throw new IllegalStateException("Missing Spark runtime system. Not able to run Spark program in Workflow.");
    }
    
    // Add classpaths for MR framework
    extraClassPaths.addAll(MapReduceContainerHelper.localizeFramework(hConf, localizeResources));

    RunId runId = ProgramRunners.getRunId(options);
    LOG.info("Launching distributed workflow: {}", program.getId().run(runId));
    TwillController controller = launcher
      .addClassPaths(extraClassPaths)
      .addDependencies(extraDependencies)
      .addEnvironment(environment)
      .launch(new WorkflowTwillApplication(program, options.getUserArguments(),
                                           workflowSpec, localizeResources, eventHandler, driverMeta.resources));
    return createProgramController(controller, program.getId(), runId);
  }

  private static YarnConfiguration createConfiguration(YarnConfiguration hConf, CConfiguration cConf,
                                                       TokenSecureStoreRenewer secureStoreRenewer) {
    YarnConfiguration configuration = new YarnConfiguration(hConf);
    configuration.setBoolean(HCONF_ATTR_CLUSTER_MODE, true);
    configuration.set("hive.metastore.token.signature", HiveAuthFactory.HS2_CLIENT_TOKEN);
    if (SecurityUtil.isKerberosEnabled(cConf)) {
      // Need to divide the interval by 0.8 because Spark logic has a 0.8 discount on the interval
      // If we don't offset it, it will look for the new credentials too soon
      // Also add 5 seconds to the interval to give master time to push the changes to the Spark client container
      configuration.setLong(HCONF_ATTR_CREDENTIALS_UPDATE_INTERVAL_MS,
                            (long) ((secureStoreRenewer.getUpdateInterval() + 5000) / 0.8));
    }

    return configuration;
  }

  /**
   * Returns the {@link DriverMeta} which includes the resource requirement for the workflow runnable due to spark
   * or MapReduce driver resources requirement. {@link DriverMeta} also contain the information about
   * whether the workflow contains spark.
   */
  private DriverMeta findDriverResources(Map<String, SparkSpecification> sparkSpecs,
                                         Map<String, MapReduceSpecification> mrSpecs,
                                         WorkflowSpecification spec) {
    // Find the resource requirements from the workflow with 768MB as minimum.
    // It is the largest memory and cores from all Spark and MapReduce programs inside the workflow
    Resources resources = new Resources(768);
    boolean hasSpark = false;

    for (WorkflowNode node : spec.getNodeIdMap().values()) {
      if (WorkflowNodeType.ACTION == node.getType()) {
        ScheduleProgramInfo programInfo = ((WorkflowActionNode) node).getProgram();
        SchedulableProgramType programType = programInfo.getProgramType();
        if (programType == SchedulableProgramType.SPARK || programType == SchedulableProgramType.MAPREDUCE) {
          // The program spec shouldn't be null, otherwise the Workflow is not valid
          Resources driverResources;
          if (programType == SchedulableProgramType.SPARK) {
            hasSpark = true;
            driverResources = sparkSpecs.get(programInfo.getProgramName()).getClientResources();
          } else {
            driverResources = mrSpecs.get(programInfo.getProgramName()).getDriverResources();
          }
          if (driverResources != null) {
            resources = max(resources, driverResources);
          }
        }
      }
    }
    return new DriverMeta(resources, hasSpark);
  }

  /**
   * Returns a {@link Resources} that has the maximum of memory and virtual cores among two Resources.
   */
  private Resources max(Resources r1, Resources r2) {
    int memory1 = r1.getMemoryMB();
    int memory2 = r2.getMemoryMB();
    int vcores1 = r1.getVirtualCores();
    int vcores2 = r2.getVirtualCores();

    if (memory1 > memory2 && vcores1 > vcores2) {
      return r1;
    }
    if (memory1 < memory2 && vcores1 < vcores2) {
      return r2;
    }
    return new Resources(Math.max(memory1, memory2),
                         Math.max(vcores1, vcores2));
  }

  /**
   * Class representing the meta information for the driver.
   */
  private static class DriverMeta {
    private final Resources resources;
    private final boolean hasSpark;

    DriverMeta(Resources resources, boolean hasSpark) {
      this.resources = resources;
      this.hasSpark = hasSpark;
    }
  }
}
