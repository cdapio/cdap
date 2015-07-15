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
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.spark.SparkContextConfig;
import co.cask.cdap.internal.app.runtime.spark.SparkUtils;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
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
  private static final int WORKFLOW_MEMORY_MB = 512;

  @Inject
  public DistributedWorkflowProgramRunner(TwillRunner twillRunner, Configuration hConf, CConfiguration cConf) {
    super(twillRunner, createConfiguration(hConf), cConf);
  }

  @Override
  protected ProgramController launch(Program program, ProgramOptions options,
                                     Map<String, File> localizeFiles, ApplicationLauncher launcher) {
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
    Resources resources = findSparkDriverResources(program.getApplicationSpecification().getSpark(), workflowSpec);
    if (resources != null) {
      // Has Spark
      File sparkAssemblyJar = SparkUtils.locateSparkAssemblyJar();
      localizeFiles.put(sparkAssemblyJar.getName(), sparkAssemblyJar);
      extraClassPaths.add(sparkAssemblyJar.getName());
    } else {
      // No Spark
      resources = new Resources();
    }

    LOG.info("Launching distributed workflow: " + program.getName() + ":" + workflowSpec.getName());
    TwillController controller = launcher.launch(
      new WorkflowTwillApplication(program, workflowSpec, localizeFiles, eventHandler, resources),
      extraClassPaths
    );
    RunId runId = RunIds.fromString(options.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    return new WorkflowTwillProgramController(program.getName(), controller, runId).startListen();
  }

  private static Configuration createConfiguration(Configuration hConf) {
    Configuration configuration = new Configuration(hConf);
    configuration.set(SparkContextConfig.HCONF_ATTR_EXECUTION_MODE, SparkContextConfig.YARN_EXECUTION_MODE);
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
