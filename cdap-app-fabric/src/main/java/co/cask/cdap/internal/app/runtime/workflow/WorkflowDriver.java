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

import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowForkBranch;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.workflow.WorkflowStatus;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.workflow.DefaultWorkflowActionSpecification;
import co.cask.cdap.internal.workflow.ProgramWorkflowAction;
import co.cask.http.NettyHttpService;
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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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
  private final ProgramWorkflowRunnerFactory workflowProgramRunnerFactory;
  private NettyHttpService httpService;
  private volatile boolean running;
  private ConcurrentHashMap<String, WorkflowStatus> status = new ConcurrentHashMap<String, WorkflowStatus>();

  WorkflowDriver(Program program, RunId runId, ProgramOptions options, InetAddress hostname,
                 WorkflowSpecification workflowSpec, ProgramRunnerFactory programRunnerFactory) {
    this.program = program;
    this.runId = runId;
    this.hostname = hostname;
    this.runtimeArgs = createRuntimeArgs(options.getUserArguments());
    this.workflowSpec = workflowSpec;
    this.logicalStartTime = options.getArguments().hasOption(ProgramOptionConstants.LOGICAL_START_TIME)
      ? Long.parseLong(options.getArguments()
                         .getOption(ProgramOptionConstants.LOGICAL_START_TIME))
      : System.currentTimeMillis();
    this.workflowProgramRunnerFactory = new ProgramWorkflowRunnerFactory(workflowSpec, programRunnerFactory, program,
                                                                         runId, options.getUserArguments(),
                                                                         logicalStartTime);
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

  private void executeWorkflowAction(ApplicationSpecification appSpec, WorkflowActionNode node,
                                     InstantiatorFactory instantiator, ClassLoader classLoader) throws Exception {

    WorkflowActionSpecification actionSpec;
    ScheduleProgramInfo actionInfo = node.getProgram();
    switch (actionInfo.getProgramType()) {
      case MAPREDUCE:
        MapReduceSpecification mapReduceSpec = appSpec.getMapReduce().get(actionInfo.getProgramName());
        String mapReduce = mapReduceSpec.getName();
        actionSpec = new DefaultWorkflowActionSpecification(new ProgramWorkflowAction(
          mapReduce, mapReduce, SchedulableProgramType.MAPREDUCE));
        break;
      case SPARK:
        SparkSpecification sparkSpec = appSpec.getSpark().get(actionInfo.getProgramName());
        String spark = sparkSpec.getName();
        actionSpec = new DefaultWorkflowActionSpecification(new ProgramWorkflowAction(
          spark, spark, SchedulableProgramType.SPARK));
        break;
      case CUSTOM_ACTION:
        actionSpec = workflowSpec.getCustomActionMap().get(actionInfo.getProgramName());
        break;
      default:
        LOG.error("Unknown Program Type '{}', Program '{}' in the Workflow.", actionInfo.getProgramType(),
                  actionInfo.getProgramName());
        throw new IllegalStateException("Workflow stopped without executing all tasks");
    }

    WorkflowStatus workflowStatus = new WorkflowStatus(state(), actionSpec, node.getNodeId());
    status.put(node.getNodeId(), workflowStatus);

    WorkflowAction action = initialize(actionSpec, classLoader, instantiator);
    try {
      ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(action.getClass().getClassLoader());
      try {
        action.run();
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }
    } catch (Throwable t) {
      LOG.warn("Exception on WorkflowAction.run(), aborting Workflow. {}", actionSpec);
      // this will always rethrow
      Throwables.propagateIfPossible(t, Exception.class);
    } finally {
      // Destroy the action.
      destroy(actionSpec, action);
    }
    status.remove(node.getNodeId());
  }

  private void executeNode(final ApplicationSpecification appSpec, WorkflowNode node,
                           final InstantiatorFactory instantiator, final ClassLoader classLoader) throws Exception {
    WorkflowNodeType nodeType = node.getType();
    switch (nodeType) {
      case ACTION:
        executeWorkflowAction(appSpec, (WorkflowActionNode) node, instantiator, classLoader);
        break;
      case FORK:
        WorkflowForkNode fork = (WorkflowForkNode) node;
        ExecutorService executorService = Executors.newCachedThreadPool();
        CompletionService<String> completionService =
          new ExecutorCompletionService<String>(executorService);

        for (final WorkflowForkBranch branch : fork.getBranches()) {
          completionService.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
              Iterator<WorkflowNode> iterator = branch.getNodes().iterator();
              while (!Thread.currentThread().isInterrupted() && iterator.hasNext() && running) {
                executeNode(appSpec, iterator.next(), instantiator, classLoader);
              }
              return branch.toString();
            }
          });
        }

        try {
          for (int i = 0; i < fork.getBranches().size(); i++) {
            Future<String> f = completionService.take();
            try {
              LOG.info("Execution of branch {} for fork {} completed", f.get(), fork);
            } catch (ExecutionException ex) {
              LOG.info("Exception occurred in the execution of the fork node {}", fork);
              throw ex;
            }
          }

        } finally {
          executorService.shutdown();
          executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.NANOSECONDS);
        }

        break;
      default:
        break;
    }
  }

  @Override
  protected void run() throws Exception {
    LOG.info("Start workflow execution for {}", workflowSpec);
    InstantiatorFactory instantiator = new InstantiatorFactory(false);
    ClassLoader classLoader = program.getClassLoader();

    // Executes actions step by step. Individually invoke the init()->run()->destroy() sequence.

    ApplicationSpecification appSpec = program.getApplicationSpecification();

    Iterator<WorkflowNode> iterator = workflowSpec.getNodes().iterator();
    while (running && iterator.hasNext()) {
      executeNode(appSpec, iterator.next(), instantiator, classLoader);
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

    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(action.getClass().getClassLoader());
    try {
      action.initialize(new BasicWorkflowContext(workflowSpec, actionSpec,
                                                 logicalStartTime,
                                                 workflowProgramRunnerFactory.getProgramWorkflowRunner(actionSpec),
                                                 runtimeArgs));
    } catch (Throwable t) {
      LOG.warn("Exception on WorkflowAction.initialize(), abort Workflow. {}", actionSpec, t);
      // this will always rethrow
      Throwables.propagateIfPossible(t, Exception.class);
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }

    return action;
  }

  /**
   * Calls the destroy method on the given WorkflowAction.
   */
  private void destroy(WorkflowActionSpecification actionSpec, WorkflowAction action) {
    ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(action.getClass().getClassLoader());
    try {
      action.destroy();
    } catch (Throwable t) {
      LOG.warn("Exception on WorkflowAction.destroy(): {}", actionSpec, t);
      // Just log, but not propagate
    } finally {
      ClassLoaders.setContextClassLoader(oldClassLoader);
    }
  }

  private Map<String, String> createRuntimeArgs(Arguments args) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : args) {
      builder.put(entry);
    }
    return builder.build();
  }

  private Supplier<Map<String, WorkflowStatus>> createStatusSupplier() {
    return new Supplier<Map<String, WorkflowStatus>>() {
      @Override
      public Map<String, WorkflowStatus> get() {
        return status;
      }
    };
  }
}
