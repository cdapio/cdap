/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.workflow.WorkflowAction;
import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.runtime.Arguments;
import com.continuuity.app.runtime.ProgramOptions;
import com.continuuity.app.runtime.workflow.WorkflowStatus;
import com.continuuity.common.lang.InstantiatorFactory;
import com.continuuity.http.NettyHttpService;
import com.continuuity.internal.app.runtime.ProgramOptionConstants;
import com.continuuity.internal.app.runtime.batch.MapReduceProgramRunner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.apache.twill.api.RunId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map;

/**
 * Core of Workflow engine that drives the execution of Workflow.
 */
final class WorkflowDriver extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowDriver.class);

  private final Program program;
  private final RunId runId;
  private final InetAddress hostname;
  private final Map<String, String> runtimeArgs;
  private final WorkflowSpecification workflowSpec;
  private final long logicalStartTime;
  private final MapReduceRunnerFactory runnerFactory;
  private NettyHttpService httpService;
  private volatile boolean running;
  private volatile WorkflowStatus workflowStatus;

  WorkflowDriver(Program program, RunId runId, ProgramOptions options, InetAddress hostname,
                 WorkflowSpecification workflowSpec, MapReduceProgramRunner programRunner) {
    this.program = program;
    this.runId = runId;
    this.hostname = hostname;
    this.runtimeArgs = createRuntimeArgs(options.getUserArguments());
    this.workflowSpec = workflowSpec;
    this.logicalStartTime = options.getArguments().hasOption(ProgramOptionConstants.LOGICAL_START_TIME)
                                ? Long.parseLong(options.getArguments()
                                                         .getOption(ProgramOptionConstants.LOGICAL_START_TIME))
                                : System.currentTimeMillis();

    this.runnerFactory = new WorkflowMapReduceRunnerFactory(workflowSpec, programRunner, program,
                                                            runId, options.getUserArguments(), logicalStartTime);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting Workflow {}", workflowSpec);

    // Using small size thread pool is enough, as the API we supported are just simple lookup.
    httpService = NettyHttpService.builder()
      .setWorkerThreadPoolSize(2)
      .setExecThreadPoolSize(4)
      .setHost(hostname.getHostName())
      .addHttpHandlers(ImmutableList.of(new WorkflowServiceHandler(createStatusSupplier())))
      .build();

    httpService.startAndWait();
    running = true;
  }

  @Override
  protected void shutDown() throws Exception {
    httpService.stopAndWait();
  }

  @Override
  protected void run() throws Exception {
    LOG.info("Start workflow execution for {}", workflowSpec);
    InstantiatorFactory instantiator = new InstantiatorFactory(false);
    ClassLoader classLoader = program.getClassLoader();

    // Executes actions step by step. Individually invoke the init()->run()->destroy() sequence.
    Iterator<WorkflowActionSpecification> iterator = workflowSpec.getActions().iterator();
    int step = 0;
    while (running && iterator.hasNext()) {
      WorkflowActionSpecification actionSpec = iterator.next();
      workflowStatus = new WorkflowStatus(state(), actionSpec, step++);

      WorkflowAction action = initialize(actionSpec, classLoader, instantiator);
      try {
        action.run();
      } catch (Throwable t) {
        LOG.warn("Exception on WorkflowAction.run(), aborting Workflow. {}", actionSpec);
        // this will always rethrow
        Throwables.propagateIfPossible(t, Exception.class);
      } finally {
        // Destroy the action.
        destroy(actionSpec, action);
      }
    }

    // If there is some task left when the loop exited, it must be called by explicit stop of this driver.
    if (iterator.hasNext()) {
      LOG.warn("Workflow explicitly stopped. Treated as abort on error. {} {}", workflowSpec);
      throw new IllegalStateException("Workflow stopped without executing all tasks: " + workflowSpec);
    }

    LOG.info("Workflow execution succeeded for {}", workflowSpec);

    running = false;
  }

  @Override
  protected void triggerShutdown() {
    running = false;
  }

  /**
   * Returns the endpoint that the http service is bind to.
   *
   * @throws IllegalStateException if the service is not started.
   */
  InetSocketAddress getServiceEndpoint() {
    Preconditions.checkState(httpService != null && httpService.isRunning(), "Workflow service is not started.");
    return httpService.getBindAddress();
  }

  /**
   * Instantiates and initialize a WorkflowAction.
   */
  @SuppressWarnings("unchecked")
  private WorkflowAction initialize(WorkflowActionSpecification actionSpec,
                                    ClassLoader classLoader, InstantiatorFactory instantiator) throws Exception {
    Class<?> clz = Class.forName(actionSpec.getClassName(), true, classLoader);
    Preconditions.checkArgument(WorkflowAction.class.isAssignableFrom(clz), "%s is not a WorkflowAction.", clz);
    WorkflowAction action = instantiator.get(TypeToken.of((Class<? extends WorkflowAction>) clz)).create();

    try {
      action.initialize(new BasicWorkflowContext(workflowSpec, actionSpec,
                                                 logicalStartTime, runnerFactory, runtimeArgs));
    } catch (Throwable t) {
      LOG.warn("Exception on WorkflowAction.initialize(), abort Workflow. {}", actionSpec, t);
      // this will always rethrow
      Throwables.propagateIfPossible(t, Exception.class);
    }

    return action;
  }

  /**
   * Calls the destroy method on the given WorkflowAction.
   */
  private void destroy(WorkflowActionSpecification actionSpec, WorkflowAction action) {
    try {
      action.destroy();
    } catch (Throwable t) {
      LOG.warn("Exception on WorkflowAction.destroy(): {}", actionSpec, t);
      // Just log, but not propagate
    }
  }

  private Map<String, String> createRuntimeArgs(Arguments args) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : args) {
      builder.put(entry);
    }
    return builder.build();
  }

  private Supplier<WorkflowStatus> createStatusSupplier() {
    return new Supplier<WorkflowStatus>() {
      @Override
      public WorkflowStatus get() {
        return workflowStatus;
      }
    };
  }
}
