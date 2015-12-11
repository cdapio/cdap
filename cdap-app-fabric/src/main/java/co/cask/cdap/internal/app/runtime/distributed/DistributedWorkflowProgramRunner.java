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
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.batch.distributed.MapReduceContainerHelper;
import co.cask.cdap.internal.app.runtime.spark.SparkContextConfig;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.security.TokenSecureStoreUpdater;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import javax.annotation.Nullable;

/**
 * A {@link ProgramRunner} to start a {@link Workflow} program in distributed mode.
 */
public final class DistributedWorkflowProgramRunner extends AbstractDistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedWorkflowProgramRunner.class);

  @Inject
  public DistributedWorkflowProgramRunner(TwillRunner twillRunner, LocationFactory locationFactory,
                                          YarnConfiguration hConf, CConfiguration cConf,
                                          TokenSecureStoreUpdater tokenSecureStoreUpdater) {
    super(twillRunner, locationFactory, createConfiguration(hConf), cConf, tokenSecureStoreUpdater);
  }

  @Override
  protected ProgramController launch(Program program, ProgramOptions options,
                                     Map<String, LocalizeResource> localizeResources,
                                     ApplicationLauncher launcher) {
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

    // See if the Workflow has Spark in it
    Resources resources = findSparkDriverResources(program.getApplicationSpecification().getSpark(), workflowSpec);
    if (resources != null) {
      // Has Spark
      File sparkAssemblyJar = SparkUtils.locateSparkAssemblyJar();
      localizeResources.put(sparkAssemblyJar.getName(), new LocalizeResource(sparkAssemblyJar));
      extraClassPaths.add(sparkAssemblyJar.getName());
    } else {
      // No Spark
      resources = new Resources();
    }
    
    // Add classpaths for MR framework
    extraClassPaths.addAll(MapReduceContainerHelper.localizeFramework(hConf, localizeResources));

    // TODO(CDAP-3119): Hack for TWILL-144. Need to remove
    File launcherFile = null;
    if (MapReduceContainerHelper.getFrameworkURI(hConf) != null) {
      File tempDir = new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                              cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile();
      tempDir.mkdirs();
      try {
        launcherFile = File.createTempFile("launcher", ".jar", tempDir);
        MapReduceContainerHelper.saveLauncher(hConf, launcherFile, extraClassPaths);
        localizeResources.put("launcher.jar", new LocalizeResource(launcherFile));
      } catch (Exception e) {
        LOG.warn("Failed to create twill container launcher.jar for TWILL-144 hack. " +
                   "Still proceed, but the run will likely fail", e);
      }
    }
    // End Hack for TWILL-144

    LOG.info("Launching distributed workflow: " + program.getName() + ":" + workflowSpec.getName());
    TwillController controller = launcher.launch(
      new WorkflowTwillApplication(program, workflowSpec, localizeResources, eventHandler, resources),
      extraClassPaths
    );

    // TODO(CDAP-3119): Hack for TWILL-144. Need to remove
    final File cleanupFile = launcherFile;
    Runnable cleanupTask = new Runnable() {
      @Override
      public void run() {
        if (cleanupFile != null) {
          cleanupFile.delete();
        }
      }
    };
    // Cleanup when the app is running. Also add a safe guide to do cleanup on terminate in case there is race
    // such that the app terminated before onRunning was called
    controller.onRunning(cleanupTask, Threads.SAME_THREAD_EXECUTOR);
    controller.onTerminated(cleanupTask, Threads.SAME_THREAD_EXECUTOR);
    // End Hack for TWILL-144

    RunId runId = RunIds.fromString(options.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    return new WorkflowTwillProgramController(program.getId(), controller, runId).startListen();
  }

  private static YarnConfiguration createConfiguration(YarnConfiguration hConf) {
    YarnConfiguration configuration = new YarnConfiguration(hConf);
    configuration.setBoolean(SparkContextConfig.HCONF_ATTR_CLUSTER_MODE, true);
    return configuration;
  }

  /**
   * Returns the {@link Resources} requirement for the workflow runnable due to spark driver resources requirement.
   * Returns {@code null} if there is no spark program in the workflow.
   */
  @Nullable
  private Resources findSparkDriverResources(Map<String, SparkSpecification> sparkSpecs, WorkflowSpecification spec) {
    // Find the resource requirements from the workflow
    // It is the largest memory and cores from all Spark program inside the workflow
    Resources resources = new Resources();
    boolean hasSpark = false;

    // Search through all workflow nodes for spark program resource requirements.
    Queue<WorkflowNode> nodes = new LinkedList<>(spec.getNodes());
    while (!nodes.isEmpty()) {
      WorkflowNode node = nodes.poll();
      switch (node.getType()) {
        case ACTION: {
          ScheduleProgramInfo programInfo = ((WorkflowActionNode) node).getProgram();
          if (programInfo.getProgramType() == SchedulableProgramType.SPARK) {
            hasSpark = true;
            // The sparkSpec shouldn't be null, otherwise the Workflow is not valid
            Resources driverResources = sparkSpecs.get(programInfo.getProgramName()).getDriverResources();
            if (driverResources != null) {
              resources = max(resources, driverResources);
            }
          }
          break;
        }
        case FORK: {
          WorkflowForkNode forkNode = (WorkflowForkNode) node;
          Iterables.addAll(nodes, Iterables.concat(forkNode.getBranches()));
          break;
        }
        case CONDITION: {
          WorkflowConditionNode conditionNode = (WorkflowConditionNode) node;
          nodes.addAll(conditionNode.getIfBranch());
          nodes.addAll(conditionNode.getElseBranch());
          break;
        }
        default:
          LOG.warn("Unknown workflow node type {}", node.getType());
      }
    }

    return hasSpark ? resources : null;
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
}
