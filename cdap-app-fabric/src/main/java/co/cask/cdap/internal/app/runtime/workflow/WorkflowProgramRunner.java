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
package co.cask.cdap.internal.app.runtime.workflow;

import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramController;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunner;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.metrics.MetricsCollectionService;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.proto.ProgramType;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.internal.RunIds;

import java.net.InetAddress;

/**
 *
 */
public class WorkflowProgramRunner implements ProgramRunner {

  private final ProgramRunnerFactory programRunnerFactory;
  private final ServiceAnnouncer serviceAnnouncer;
  private final InetAddress hostname;
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  public WorkflowProgramRunner(ProgramRunnerFactory programRunnerFactory,
                               ServiceAnnouncer serviceAnnouncer,
                               @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                               MetricsCollectionService metricsCollectionService) {
    this.programRunnerFactory = programRunnerFactory;
    this.serviceAnnouncer = serviceAnnouncer;
    this.hostname = hostname;
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify options
    ApplicationSpecification appSpec = program.getApplicationSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.WORKFLOW, "Only WORKFLOW process type is supported.");

    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(program.getName());
    Preconditions.checkNotNull(workflowSpec, "Missing WorkflowSpecification for %s", program.getName());

    RunId runId = RunIds.generate();
    WorkflowDriver driver = new WorkflowDriver(program, runId, options, hostname, workflowSpec, programRunnerFactory);

    // Controller needs to be created before starting the driver so that the state change of the driver
    // service can be fully captured by the controller.
    ProgramController controller = new WorkflowProgramController(program, driver, serviceAnnouncer, runId);
    driver.start();

    return controller;
  }
}
