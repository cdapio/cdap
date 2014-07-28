/*
 * Copyright 2012-2014 Continuuity, Inc.
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
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.ProgramController;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.ProgramRunner;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsCollectionService;
import com.continuuity.internal.app.runtime.batch.MapReduceProgramRunner;
import com.continuuity.proto.ProgramType;
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

  private final MapReduceProgramRunner mapReduceProgramRunner;
  private final ServiceAnnouncer serviceAnnouncer;
  private final InetAddress hostname;
  private final MetricsCollectionService metricsCollectionService;

  @Inject
  public WorkflowProgramRunner(MapReduceProgramRunner mapReduceProgramRunner,
                               ServiceAnnouncer serviceAnnouncer,
                               @Named(Constants.AppFabric.SERVER_ADDRESS) InetAddress hostname,
                               MetricsCollectionService metricsCollectionService) {
    this.mapReduceProgramRunner = mapReduceProgramRunner;
    this.serviceAnnouncer = serviceAnnouncer;
    this.hostname = hostname;
    this.metricsCollectionService = metricsCollectionService;
  }

  @Override
  public ProgramController run(Program program, ProgramOptions options) {
    // Extract and verify options
    ApplicationSpecification appSpec = program.getSpecification();
    Preconditions.checkNotNull(appSpec, "Missing application specification.");

    ProgramType processorType = program.getType();
    Preconditions.checkNotNull(processorType, "Missing processor type.");
    Preconditions.checkArgument(processorType == ProgramType.WORKFLOW, "Only WORKFLOW process type is supported.");

    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(program.getName());
    Preconditions.checkNotNull(workflowSpec, "Missing WorkflowSpecification for %s", program.getName());

    RunId runId = RunIds.generate();
    WorkflowDriver driver = new WorkflowDriver(program, runId, options, hostname, workflowSpec, mapReduceProgramRunner);

    // Controller needs to be created before starting the driver so that the state change of the driver
    // service can be fully captured by the controller.
    ProgramController controller = new WorkflowProgramController(program, driver, serviceAnnouncer, runId);
    driver.start();

    return controller;
  }
}
