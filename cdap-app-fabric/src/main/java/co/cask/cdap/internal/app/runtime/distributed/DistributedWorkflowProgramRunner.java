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
import co.cask.cdap.app.program.Programs;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.io.Closeables;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunner;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link ProgramRunner} to start a {@link Workflow} program in distributed mode.
 */
public final class DistributedWorkflowProgramRunner extends DistributedProgramRunner {

  private final ProgramRunnerFactory programRunnerFactory;

  @Inject
  DistributedWorkflowProgramRunner(TwillRunner twillRunner, YarnConfiguration hConf, CConfiguration cConf,
                                   TokenSecureStoreRenewer tokenSecureStoreRenewer,
                                   ProgramRunnerFactory programRunnerFactory,
                                   Impersonator impersonator) {
    super(twillRunner, hConf, cConf, tokenSecureStoreRenewer, impersonator);
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
  public ProgramController createProgramController(TwillController twillController,
                                                   ProgramDescriptor programDescriptor, RunId runId) {
    return new WorkflowTwillProgramController(programDescriptor.getProgramId(), twillController, runId).startListen();
  }

  @Override
  protected void setupLaunchConfig(LaunchConfig launchConfig, Program program, ProgramOptions options,
                                   CConfiguration cConf, Configuration hConf, File tempDir) throws IOException {

    WorkflowSpecification spec = program.getApplicationSpecification().getWorkflows().get(program.getName());
    List<ClassAcceptor> acceptors = new ArrayList<>();

    // Only interested in MapReduce and Spark nodes
    Set<SchedulableProgramType> runnerTypes = EnumSet.of(SchedulableProgramType.MAPREDUCE,
                                                         SchedulableProgramType.SPARK);
    for (WorkflowActionNode node : Iterables.filter(spec.getNodeIdMap().values(), WorkflowActionNode.class)) {
      // For each type, we only need one node to setup the launch context
      ScheduleProgramInfo programInfo = node.getProgram();
      if (!runnerTypes.remove(programInfo.getProgramType())) {
        continue;
      }

      // Find the ProgramRunner of the given type and setup the launch context
      ProgramType programType = ProgramType.valueOfSchedulableType(programInfo.getProgramType());
      ProgramRunner runner = programRunnerFactory.create(programType);
      try {
        if (runner instanceof DistributedProgramRunner) {
          // Call setupLaunchConfig with the corresponding program
          ProgramId programId = program.getId().getParent().program(programType, programInfo.getProgramName());
          ((DistributedProgramRunner) runner).setupLaunchConfig(launchConfig,
                                                                Programs.create(cConf, program, programId, runner),
                                                                options, cConf, hConf, tempDir);
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

    // Clear and set the runnable for the workflow driver
    launchConfig.clearRunnables();
    Resources resources = findDriverResources(program.getApplicationSpecification().getSpark(),
                                              program.getApplicationSpecification().getMapReduce(), spec);

    resources = SystemArguments.getResources(options.getUserArguments(), resources);
    launchConfig.addRunnable(spec.getName(), new WorkflowTwillRunnable(spec.getName()), resources, 1, 0);
  }

  /**
   * Returns the {@link Resources} requirement for the workflow runnable deduced by Spark
   * or MapReduce driver resources requirement.
   */
  private Resources findDriverResources(Map<String, SparkSpecification> sparkSpecs,
                                        Map<String, MapReduceSpecification> mrSpecs,
                                        WorkflowSpecification spec) {
    // Find the resource requirements from the workflow with 768MB as minimum.
    // It is the largest memory and cores from all Spark and MapReduce programs inside the workflow
    Resources resources = new Resources(768);

    for (WorkflowNode node : spec.getNodeIdMap().values()) {
      if (WorkflowNodeType.ACTION == node.getType()) {
        ScheduleProgramInfo programInfo = ((WorkflowActionNode) node).getProgram();
        SchedulableProgramType programType = programInfo.getProgramType();
        if (programType == SchedulableProgramType.SPARK || programType == SchedulableProgramType.MAPREDUCE) {
          // The program spec shouldn't be null, otherwise the Workflow is not valid
          Resources driverResources;
          if (programType == SchedulableProgramType.SPARK) {
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
    return resources;
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
