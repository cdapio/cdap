/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.app.program.Program;
import com.continuuity.internal.app.runtime.AbstractProgramController;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.RunId;
import org.apache.twill.api.ServiceAnnouncer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.ServiceListenerAdapter;
import org.apache.twill.common.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 *
 */
final class WorkflowProgramController extends AbstractProgramController {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowProgramController.class);

  private final WorkflowDriver driver;
  private final String serviceName;
  private final ServiceAnnouncer serviceAnnouncer;
  private Cancellable cancelAnnounce;

  WorkflowProgramController(Program program, WorkflowDriver driver, ServiceAnnouncer serviceAnnouncer, RunId runId) {
    super(program.getName(), runId);
    this.driver = driver;
    this.serviceName = getServiceName(program);
    this.serviceAnnouncer = serviceAnnouncer;
    startListen(driver);
  }

  @Override
  protected void doSuspend() throws Exception {
    LOG.info("Suspend not supported.");
  }

  @Override
  protected void doResume() throws Exception {
    LOG.info("Resume not supported.");
  }

  @Override
  protected void doStop() throws Exception {
    driver.stopAndWait();
  }

  @Override
  protected void doCommand(String name, Object value) throws Exception {
    LOG.info("Command ignored {}, {}", name, value);
  }
  
  private void startListen(Service service) {
    // Forward state changes from the given service to this controller.
    service.addListener(new ServiceListenerAdapter() {
      @Override
      public void running() {
        InetSocketAddress endpoint = driver.getServiceEndpoint();
        cancelAnnounce = serviceAnnouncer.announce(serviceName, endpoint.getPort());
        LOG.info("Workflow service {} announced at {}", serviceName, endpoint);
        started();
      }

      @Override
      public void terminated(Service.State from) {
        LOG.info("Workflow service terminated from {}. Un-registering service {}.", from, serviceName);
        cancelAnnounce.cancel();
        LOG.info("Service {} unregistered.", serviceName);
        stop();
      }

      @Override
      public void failed(Service.State from, Throwable failure) {
        LOG.info("Workflow service failed from {}. Un-registering service {}.", from, serviceName, failure);
        cancelAnnounce.cancel();
        LOG.info("Service {} unregistered.", serviceName);
        error(failure);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private String getServiceName(Program program) {
    return String.format("workflow.%s.%s.%s",
                         program.getAccountId(), program.getApplicationId(), program.getName());
  }
}
