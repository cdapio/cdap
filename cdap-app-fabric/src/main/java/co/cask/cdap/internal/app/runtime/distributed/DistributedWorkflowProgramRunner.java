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
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.guice.ClusterMode;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.SimpleProgramOptions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link ProgramRunner} to start a {@link Workflow} program in distributed mode.
 */
public final class DistributedWorkflowProgramRunner extends DistributedProgramRunner {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedWorkflowProgramRunner.class);

  private final ProgramRunnerFactory programRunnerFactory;

  @Inject
  DistributedWorkflowProgramRunner(CConfiguration cConf, YarnConfiguration hConf,
                                   Impersonator impersonator, ClusterMode clusterMode,
                                   @Constants.AppFabric.ProgramRunner TwillRunner twillRunner,
                                   @Constants.AppFabric.ProgramRunner ProgramRunnerFactory programRunnerFactory) {
    super(cConf, hConf, impersonator, clusterMode, twillRunner);
    this.programRunnerFactory = programRunnerFactory;
  }

  @Override
  protected void validateOptions(Program program, ProgramOptions options) {
    super.validateOptions(program, options);

    // Extract and verify parameters
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.WORKFLOW, "Only WORKFLOW process type is supported.");

    WorkflowSpecification spec = appSpec.getWorkflows().get(program.getName());
    Preconditions.checkNotNull(spec, "Missing WorkflowSpecification for %s", program.getName());

    for (WorkflowNode node : spec.getNodes()) {
      if (node.getType().equals(WorkflowNodeType.ACTION)) {
        SystemArguments.validateTransactionTimeout(options.getUserArguments().asMap(),
                                                   cConf, "action", node.getNodeId());
      }
    }
  }

  @Override
  protected ProgramController createProgramController(TwillController twillController,
                                                      ProgramDescriptor programDescriptor, RunId runId) {
    return new WorkflowTwillProgramController(programDescriptor.getProgramId(), twillController, runId).startListen();
  }

  @Override
  protected void setupLaunchConfig(ProgramLaunchConfig launchConfig, Program program, ProgramOptions options,
                                   CConfiguration cConf, Configuration hConf, File tempDir) throws IOException {

    WorkflowSpecification spec = program.getApplicationSpecification().getWorkflows().get(program.getName());
    List<ClassAcceptor> acceptors = new ArrayList<>();

    // Only interested in MapReduce and Spark nodes.
    // This is because CUSTOM_ACTION types are running inside the driver
    Set<SchedulableProgramType> runnerTypes = EnumSet.of(SchedulableProgramType.MAPREDUCE,
                                                         SchedulableProgramType.SPARK);

    Iterable<ScheduleProgramInfo> programInfos = spec.getNodeIdMap().values().stream()
      .filter(WorkflowActionNode.class::isInstance)
      .map(WorkflowActionNode.class::cast)
      .map(WorkflowActionNode::getProgram)
      .filter(programInfo -> runnerTypes.contains(programInfo.getProgramType()))::iterator;

    // Can't use Stream.forEach as we want to preserve the IOException being thrown
    for (ScheduleProgramInfo programInfo : programInfos) {
      ProgramType programType = ProgramType.valueOfSchedulableType(programInfo.getProgramType());
      ProgramRunner runner = programRunnerFactory.create(programType);
      try {
        if (runner instanceof DistributedProgramRunner) {
          // Call setupLaunchConfig with the corresponding program.
          // Need to constructs a new ProgramOptions with the scope extracted for the given program
          ProgramId programId = program.getId().getParent().program(programType, programInfo.getProgramName());
          Map<String, String> programUserArgs = RuntimeArguments.extractScope(programId.getType().getScope(),
                                                                              programId.getProgram(),
                                                                              options.getUserArguments().asMap());
          ProgramOptions programOptions = new SimpleProgramOptions(programId, options.getArguments(),
                                                                   new BasicArguments(programUserArgs));

          ((DistributedProgramRunner) runner).setupLaunchConfig(launchConfig,
                                                                Programs.create(cConf, program, programId, runner),
                                                                programOptions, cConf, hConf, tempDir);
          acceptors.add(launchConfig.getClassAcceptor());
        }
      } finally {
        if (runner instanceof Closeable) {
          Closeables.closeQuietly((Closeable) runner);
        }
      }
    }

    // Set the class acceptor
    launchConfig.setClassAcceptor(new AndClassAcceptor(acceptors));

    // Find out the default resources requirements based on the programs inside the workflow
    // At least gives the Workflow driver 768 mb of container memory
    Map<String, Resources> runnablesResources = Maps.transformValues(launchConfig.getRunnables(), this::getResources);
    Resources defaultResources = maxResources(new Resources(768),
                                              findDriverResources(spec.getNodes(), runnablesResources));

    // Clear and set the runnable for the workflow driver.
    launchConfig.clearRunnables();
    // Extract scoped runtime arguments that only meant for the workflow but not for child nodes
    Map<String, String> runtimeArgs = RuntimeArguments.extractScope("task", "workflow",
                                                                    options.getUserArguments().asMap());
    launchConfig.addRunnable(spec.getName(), new WorkflowTwillRunnable(spec.getName()), 1,
                             runtimeArgs, defaultResources, 0);
  }

  /**
   * Returns the {@link Resources} requirement for the workflow runnable deduced by Spark
   * or MapReduce driver resources requirement.
   */
  private Resources findDriverResources(Collection<WorkflowNode> nodes, Map<String, Resources> runnablesResources) {
    // Find the resource requirements for the workflow based on the nodes memory requirements
    Resources resources = new Resources();

    for (WorkflowNode node : nodes) {
      switch (node.getType()) {
        case ACTION:
          String programName = ((WorkflowActionNode) node).getProgram().getProgramName();
          Resources runnableResources = runnablesResources.get(programName);
          if (runnableResources != null) {
            resources = maxResources(resources, runnableResources);
          }
          break;
        case FORK:
          Resources forkResources = ((WorkflowForkNode) node).getBranches().stream()
            .map(branches -> findDriverResources(branches, runnablesResources))
            .reduce(this::mergeForkResources)
            .orElse(resources);
          resources = maxResources(resources, forkResources);
          break;
        case CONDITION:
          Resources branchesResources =
            maxResources(findDriverResources(((WorkflowConditionNode) node).getIfBranch(), runnablesResources),
                         findDriverResources(((WorkflowConditionNode) node).getElseBranch(), runnablesResources));

          resources = maxResources(resources, branchesResources);
          break;
        default:
          // This shouldn't happen unless we add new node type
          LOG.warn("Ignoring unsupported Workflow node type {}", node.getType());
      }
    }

    return resources;
  }

  /**
   * Returns a {@link Resources} that has the maximum of memory and virtual cores among two Resources.
   */
  private Resources maxResources(Resources r1, Resources r2) {
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
   * Merges resources usage across two branches in a fork. It returns a new {@link Resources} that
   * sum up the memory and take the max of the vcores.
   */
  private Resources mergeForkResources(Resources r1, Resources r2) {
    return new Resources(r1.getMemoryMB() + r2.getMemoryMB(), Math.max(r1.getVirtualCores(), r2.getVirtualCores()));
  }

  private Resources getResources(RunnableDefinition runnableDefinition) {
    ResourceSpecification resourceSpec = runnableDefinition.getResources();
    return new Resources(resourceSpec.getMemorySize(), resourceSpec.getVirtualCores());
  }

  /**
   * A {@link ClassAcceptor} that accepts if and only if a list of acceptors all accept.
   */
  private static final class AndClassAcceptor extends ClassAcceptor {

    private final List<ClassAcceptor> acceptors;

    private AndClassAcceptor(List<ClassAcceptor> acceptors) {
      this.acceptors = acceptors;
    }

    @Override
    public boolean accept(String className, URL classUrl, URL classPathUrl) {
      for (ClassAcceptor acceptor : acceptors) {
        if (!acceptor.accept(className, classUrl, classPathUrl)) {
          return false;
        }
      }
      return true;
    }
  }
}
