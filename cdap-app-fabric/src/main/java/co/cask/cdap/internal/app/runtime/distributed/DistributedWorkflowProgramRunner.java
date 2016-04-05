/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRuntimeProvider;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import co.cask.cdap.internal.app.runtime.batch.distributed.MapReduceContainerHelper;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.security.TokenSecureStoreUpdater;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.hadoop.mapred.YarnClientProtocolProvider;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A {@link ProgramRunner} to start a {@link Workflow} program in distributed mode.
 */
public final class DistributedWorkflowProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedWorkflowProgramRunner.class);

  private static final String HCONF_ATTR_CLUSTER_MODE = "cdap.spark.cluster.mode";

  private final ProgramRuntimeProviderLoader runtimeProviderLoader;

  @Inject
  public DistributedWorkflowProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf, CConfiguration cConf,
                                          TokenSecureStoreUpdater tokenSecureStoreUpdater,
                                          ProgramRuntimeProviderLoader runtimeProviderLoader) {
    super(twillRunner, createConfiguration(hConf), cConf, tokenSecureStoreUpdater);
    this.runtimeProviderLoader = runtimeProviderLoader;
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

    // Adds the extra classes that MapReduce needs
    extraDependencies.add(YarnClientProtocolProvider.class);

    // See if the Workflow has Spark or MapReduce in it
    DriverMeta driverMeta = findDriverResources(program.getApplicationSpecification().getSpark(),
                                                program.getApplicationSpecification().getMapReduce(), workflowSpec);

    if (driverMeta.hasSpark) {
      // Adds the extra class that Spark runtime needed
      ProgramRuntimeProvider provider = runtimeProviderLoader.get(ProgramType.SPARK);
      Preconditions.checkState(provider != null, "Missing Spark runtime system. Not able to run Spark program.");
      extraDependencies.add(provider.getClass());

      // Localize the spark-assembly jar and spark conf zip
      String sparkAssemblyJarName = SparkUtils.prepareSparkResources(cConf, tempDir, localizeResources);
      extraClassPaths.add(sparkAssemblyJarName);
    }
    
    // Add classpaths for MR framework
    extraClassPaths.addAll(MapReduceContainerHelper.localizeFramework(hConf, localizeResources));

    LOG.info("Launching distributed workflow: " + program.getName() + ":" + workflowSpec.getName());
    TwillController controller = launcher.launch(
      new WorkflowTwillApplication(program, workflowSpec, localizeResources, eventHandler, driverMeta.resources),
      extraClassPaths, extraDependencies
    );
    RunId runId = RunIds.fromString(options.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    return new WorkflowTwillProgramController(program.getId(), controller, runId).startListen();
  }

  private static YarnConfiguration createConfiguration(YarnConfiguration hConf) {
    YarnConfiguration configuration = new YarnConfiguration(hConf);
    configuration.setBoolean(HCONF_ATTR_CLUSTER_MODE, true);
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
    // Find the resource requirements from the workflow
    // It is the largest memory and cores from all Spark and MapReduce programs inside the workflow
    Resources resources = new Resources();
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
            driverResources = sparkSpecs.get(programInfo.getProgramName()).getDriverResources();
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
