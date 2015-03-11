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
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.app.ApplicationSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.internal.workflow.DefaultWorkflowActionSpecification;
import co.cask.cdap.internal.workflow.ProgramWorkflowAction;
import co.cask.http.NettyHttpService;
import com.clearspring.analytics.util.Lists;
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
import java.util.List;
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
  private volatile Thread runningThread;
  private final Map<String, WorkflowActionNode> status = new ConcurrentHashMap<String, WorkflowActionNode>();

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
    runningThread = Thread.currentThread();
  }

  @Override
  protected void shutDown() throws Exception {
    httpService.stopAndWait();
  }

  private void executeAction(ApplicationSpecification appSpec, WorkflowActionNode node,
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
        actionSpec = node.getActionSpecification();
        break;
      default:
        LOG.error("Unknown Program Type '{}', Program '{}' in the Workflow.", actionInfo.getProgramType(),
                  actionInfo.getProgramName());
        throw new IllegalStateException("Workflow stopped without executing all tasks");
    }

    status.put(node.getNodeId(), node);

    WorkflowAction action = initialize(actionSpec, classLoader, instantiator);
    try {
      ClassLoader oldClassLoader = ClassLoaders.setContextClassLoader(action.getClass().getClassLoader());
      try {
        action.run();
      } finally {
        ClassLoaders.setContextClassLoader(oldClassLoader);
      }
    } catch (Throwable t) {
      LOG.error("Exception on WorkflowAction.run(), aborting Workflow. {}", actionSpec);
      Throwables.propagateIfPossible(t, Exception.class);
      throw Throwables.propagate(t);
    } finally {
      // Destroy the action.
      destroy(actionSpec, action);
      status.remove(node.getNodeId());
    }
  }

  private void executeFork(final ApplicationSpecification appSpec, WorkflowForkNode fork,
                           final InstantiatorFactory instantiator, final ClassLoader classLoader) throws Exception {
    ExecutorService executorService = Executors.newFixedThreadPool(fork.getBranches().size());
    CompletionService<String> completionService = new ExecutorCompletionService<String>(executorService);
    try {
      for (final List<WorkflowNode> branch : fork.getBranches()) {
        completionService.submit(new Callable<String>() {
          @Override
          public String call() throws Exception {
            Iterator<WorkflowNode> iterator = branch.iterator();
            while (!Thread.currentThread().isInterrupted() && iterator.hasNext()) {
              executeNode(appSpec, iterator.next(), instantiator, classLoader);
            }
            return branch.toString();
          }
        });
      }

      for (int i = 0; i < fork.getBranches().size(); i++) {
        try {
          Future<String> f = completionService.take();
          String branchInfo = f.get();
          LOG.info("Execution of branch {} for fork {} completed", branchInfo, fork);
        } catch (Throwable t) {
          Throwable rootCause = Throwables.getRootCause(t);
          if (rootCause instanceof ExecutionException) {
            LOG.error("Exception occurred in the execution of the fork node {}", fork);
            throw (ExecutionException) t;
          }
          if (rootCause instanceof InterruptedException) {
            LOG.error("Workflow execution aborted.");
            break;
          }
          Throwables.propagateIfPossible(t, Exception.class);
          throw Throwables.propagate(t);
        }
      }
    } finally {
      executorService.shutdownNow();
      executorService.awaitTermination(Integer.MAX_VALUE, TimeUnit.NANOSECONDS);
    }
  }

  private void executeNode(final ApplicationSpecification appSpec, WorkflowNode node,
                           final InstantiatorFactory instantiator, final ClassLoader classLoader) throws Exception {
    WorkflowNodeType nodeType = node.getType();
    switch (nodeType) {
      case ACTION:
        executeAction(appSpec, (WorkflowActionNode) node, instantiator, classLoader);
        break;
      case FORK:
        executeFork(appSpec, (WorkflowForkNode) node, instantiator, classLoader);
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

    final ApplicationSpecification appSpec = program.getApplicationSpecification();
    final Iterator<WorkflowNode> iterator = workflowSpec.getNodes().iterator();
    while (iterator.hasNext() && runningThread != null) {
      try {
        executeNode(appSpec, iterator.next(), instantiator, classLoader);
      } catch (Throwable t) {
        Throwables.propagate(t);
      }
    }

    LOG.info("Workflow execution succeeded for {}", workflowSpec);
  }

  @Override
  protected void triggerShutdown() {
    Thread t = runningThread;
    runningThread = null;
    t.interrupt();
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

  private Supplier<List<WorkflowActionNode>> createStatusSupplier() {
    return new Supplier<List<WorkflowActionNode>>() {
      @Override
      public List<WorkflowActionNode> get() {
        List<WorkflowActionNode> currentNodes = Lists.newArrayList();
        for (Map.Entry<String, WorkflowActionNode> entry : status.entrySet()) {
          currentNodes.add(entry.getValue());
        }
        return currentNodes;
      }
    };
  }
}
