/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.api.Predicate;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.schedule.SchedulableProgramType;
import co.cask.cdap.api.workflow.ScheduleProgramInfo;
import co.cask.cdap.api.workflow.WorkflowAction;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowActionSpecification;
import co.cask.cdap.api.workflow.WorkflowConditionNode;
import co.cask.cdap.api.workflow.WorkflowContext;
import co.cask.cdap.api.workflow.WorkflowForkNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowNodeType;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.Arguments;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramRunnerFactory;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.lang.ClassLoaders;
import co.cask.cdap.common.lang.CombineClassLoader;
import co.cask.cdap.common.lang.InstantiatorFactory;
import co.cask.cdap.common.lang.PropertyFieldSetter;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.registry.UsageRegistry;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.DataSetFieldSetter;
import co.cask.cdap.internal.app.runtime.MetricsFieldSetter;
import co.cask.cdap.internal.app.runtime.ProgramOptionConstants;
import co.cask.cdap.internal.app.workflow.DefaultWorkflowActionConfigurer;
import co.cask.cdap.internal.dataset.DatasetCreationSpec;
import co.cask.cdap.internal.lang.Reflections;
import co.cask.cdap.internal.workflow.ProgramWorkflowAction;
import co.cask.cdap.logging.context.WorkflowLoggingContext;
import co.cask.cdap.proto.Id;
import co.cask.http.NettyHttpService;
import co.cask.tephra.TransactionContext;
import co.cask.tephra.TransactionFailureException;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.twill.api.RunId;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Core of Workflow engine that drives the execution of Workflow.
 */
final class WorkflowDriver extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowDriver.class);

  private final Program program;
  private final InetAddress hostname;
  private final Map<String, String> runtimeArgs;
  private final WorkflowSpecification workflowSpec;
  private final long logicalStartTime;
  private final ProgramWorkflowRunnerFactory workflowProgramRunnerFactory;
  private final Map<String, WorkflowActionNode> status = new ConcurrentHashMap<>();
  private final LoggingContext loggingContext;
  private NettyHttpService httpService;
  private volatile Thread runningThread;
  private boolean suspended;
  private Lock lock;
  private Condition condition;
  private final RunId runId;
  private final MetricsCollectionService metricsCollectionService;
  private final NameMappedDatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final Store store;
  private final Id.Workflow workflowId;
  private final CConfiguration cConf;

  WorkflowDriver(Program program, ProgramOptions options, InetAddress hostname,
                 WorkflowSpecification workflowSpec, ProgramRunnerFactory programRunnerFactory,
                 MetricsCollectionService metricsCollectionService,
                 DatasetFramework datasetFramework, DiscoveryServiceClient discoveryServiceClient,
                 TransactionSystemClient txClient, Store store, CConfiguration cConf) {
    this.program = program;
    this.hostname = hostname;
    this.runtimeArgs = createRuntimeArgs(options.getUserArguments());
    this.workflowSpec = workflowSpec;
    Arguments arguments = options.getArguments();
    this.logicalStartTime = arguments.hasOption(ProgramOptionConstants.LOGICAL_START_TIME)
      ? Long.parseLong(arguments.getOption(ProgramOptionConstants.LOGICAL_START_TIME))
      : System.currentTimeMillis();
    this.lock = new ReentrantLock();
    this.condition = lock.newCondition();
    this.runId = RunIds.fromString(options.getArguments().getOption(ProgramOptionConstants.RUN_ID));
    this.loggingContext = new WorkflowLoggingContext(program.getNamespaceId(), program.getApplicationId(),
                                                     program.getName(), runId.getId());

    this.metricsCollectionService = metricsCollectionService;
    this.datasetFramework = new NameMappedDatasetFramework(datasetFramework,
                                                           workflowSpec.getLocalDatasetSpecs().keySet(),
                                                           runId.getId());
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.store = store;
    this.workflowId = Id.Workflow.from(program.getId().getApplication(), workflowSpec.getName());
    this.cConf = cConf;
    this.workflowProgramRunnerFactory = new ProgramWorkflowRunnerFactory(workflowSpec, programRunnerFactory,
                                                                         program, options);
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(loggingContext);
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
    createLocalDatasets();
  }

  private void blockIfSuspended() {
    lock.lock();
    try {
      while (suspended) {
        condition.await();
      }
    } catch (InterruptedException e) {
      LOG.warn("Wait on the Condition is interrupted.");
      Thread.currentThread().interrupt();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Suspends the execution of the Workflow after the currently running actions complete.
   * @throws Exception
   */
  public void suspend() throws Exception {
    LOG.info("Suspending the Workflow");
    lock.lock();
    try {
      suspended = true;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Resumes the execution of the Workflow.
   * @throws Exception
   */
  public void resume() throws Exception {
    LOG.info("Resuming the Workflow");
    lock.lock();
    try {
      suspended = false;
      condition.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    httpService.stopAndWait();
    deleteLocalDatasets();
  }

  private void executeAction(WorkflowActionNode node,
                             InstantiatorFactory instantiator, final ClassLoader classLoader,
                             WorkflowToken token) throws Exception {

    final SchedulableProgramType programType = node.getProgram().getProgramType();
    final WorkflowActionSpecification actionSpec = getActionSpecification(node, programType);

    status.put(node.getNodeId(), node);

    final BasicWorkflowContext workflowContext = createWorkflowContext(actionSpec, token, node.getNodeId());
    final WorkflowAction action = initialize(actionSpec, classLoader, instantiator, workflowContext);

    CountDownLatch executorTerminateLatch = new CountDownLatch(1);
    ExecutorService executorService = createExecutor(1, executorTerminateLatch, "action-" + node.getNodeId() + "-%d");

    try {
      // Run the action in new thread
      Future<?> future = executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          setContextCombinedClassLoader(action);
          try {
            if (programType == SchedulableProgramType.CUSTOM_ACTION) {
              runInTransaction(action, workflowContext);
            } else {
              action.run();
            }
            return null;
          } finally {
            destroyInTransaction(action, actionSpec, workflowContext);
          }
        }
      });
      future.get();
    } catch (Throwable t) {
      LOG.error("Exception on WorkflowAction.run(), aborting Workflow. {}", actionSpec, t);
      Throwables.propagateIfPossible(t, Exception.class);
      throw Throwables.propagate(t);
    } finally {
      executorService.shutdownNow();
      executorTerminateLatch.await();
      status.remove(node.getNodeId());
    }
    store.updateWorkflowToken(workflowId, runId.getId(), token);
  }

  private WorkflowActionSpecification getActionSpecification(WorkflowActionNode node,
                                                             SchedulableProgramType programType) {
    WorkflowActionSpecification actionSpec;
    ScheduleProgramInfo actionInfo = node.getProgram();
    switch (programType) {
      case MAPREDUCE:
      case SPARK:
        actionSpec = DefaultWorkflowActionConfigurer.configureAction(
          new ProgramWorkflowAction(actionInfo.getProgramName(), programType));
        break;
      case CUSTOM_ACTION:
        actionSpec = node.getActionSpecification();
        break;
      default:
        LOG.error("Unknown Program Type '{}', Program '{}' in the Workflow.", actionInfo.getProgramType(),
                  actionInfo.getProgramName());
        throw new IllegalStateException("Workflow stopped without executing all tasks");
    }
    return actionSpec;
  }

  private void runInTransaction(WorkflowAction action,
                                BasicWorkflowContext workflowContext) throws TransactionFailureException {
    TransactionContext transactionContext = workflowContext.getDatasetCache().newTransactionContext();
    transactionContext.start();
    try {
      action.run();
    } catch (Throwable e) {
      LOG.error("Transaction failed to run: " + e.getMessage());
      transactionContext.abort(new TransactionFailureException("Transaction function failure for transaction. ", e));
    }
    transactionContext.finish();
  }

  private void destroyInTransaction(WorkflowAction action, WorkflowActionSpecification spec,
                                    BasicWorkflowContext workflowContext) throws TransactionFailureException {
    TransactionContext transactionContext = workflowContext.getDatasetCache().newTransactionContext();
    transactionContext.start();
    try {
      try {
        destroy(spec, action);
      } catch (Throwable e) {
        LOG.error("Transaction failed to destroy: " + e.getMessage());
        transactionContext.abort(new TransactionFailureException("Transaction function failure for transaction. ", e));
      }
      transactionContext.finish();
    } finally {
      // after the action is destroyed, we can release its datasets; TODO: would it be better to hold on to them?
      workflowContext.getDatasetCache().invalidate();
    }
  }

  private void initializeInTransaction(WorkflowAction action, BasicWorkflowContext workflowContext) throws Exception {
    TransactionContext transactionContext = workflowContext.getDatasetCache().newTransactionContext();
    transactionContext.start();
    try {
      action.initialize(workflowContext);
    } catch (Throwable e) {
      LOG.error("Transaction failed to initialize: " + e.getMessage());
      transactionContext.abort(new TransactionFailureException("Transaction function failure for transaction. ", e));
    }
    transactionContext.finish();
  }

  private void executeFork(final ApplicationSpecification appSpec, WorkflowForkNode fork,
                           final InstantiatorFactory instantiator, final ClassLoader classLoader,
                           final WorkflowToken token) throws Exception {

    CountDownLatch executorTerminateLatch = new CountDownLatch(1);
    ExecutorService executorService = createExecutor(fork.getBranches().size(), executorTerminateLatch,
                                                     "fork-" + fork.getNodeId() + "-%d");
    CompletionService<Map.Entry<String, WorkflowToken>> completionService =
      new ExecutorCompletionService<>(executorService);

    try {
      for (final List<WorkflowNode> branch : fork.getBranches()) {
        completionService.submit(new Callable<Map.Entry<String, WorkflowToken>>() {
          @Override
          public Map.Entry<String, WorkflowToken> call() throws Exception {
            WorkflowToken copiedToken = ((BasicWorkflowToken) token).deepCopy();
            executeAll(branch.iterator(), appSpec, instantiator, classLoader, copiedToken);
            return Maps.immutableEntry(branch.toString(), copiedToken);
          }
        });
      }

      for (int i = 0; i < fork.getBranches().size(); i++) {
        try {
          Future<Map.Entry<String, WorkflowToken>> forkBranchResult = completionService.take();
          Map.Entry<String, WorkflowToken> retValue = forkBranchResult.get();
          String branchInfo = retValue.getKey();
          WorkflowToken branchToken = retValue.getValue();
          ((BasicWorkflowToken) token).mergeToken((BasicWorkflowToken) branchToken);
          LOG.info("Execution of branch {} for fork {} completed", branchInfo, fork);
        } catch (Throwable t) {
          Throwable rootCause = Throwables.getRootCause(t);
          if (rootCause instanceof ExecutionException) {
            LOG.error("Exception occurred in the execution of the fork node {}.", fork, rootCause);
            throw (ExecutionException) rootCause;
          }
          if (rootCause instanceof InterruptedException) {
            LOG.error("Workflow execution aborted.", rootCause);
            break;
          }
          Throwables.propagateIfPossible(rootCause, Exception.class);
          throw Throwables.propagate(rootCause);
        }
      }
    } finally {
      // Update the WorkflowToken after the execution of the FORK node completes.
      store.updateWorkflowToken(workflowId, runId.getId(), token);
      executorService.shutdownNow();
      // Wait for the executor termination
      executorTerminateLatch.await();
    }
  }

  private void executeNode(ApplicationSpecification appSpec, WorkflowNode node, InstantiatorFactory instantiator,
                           ClassLoader classLoader, WorkflowToken token) throws Exception {
    WorkflowNodeType nodeType = node.getType();
    ((BasicWorkflowToken) token).setCurrentNode(node.getNodeId());
    switch (nodeType) {
      case ACTION:
        executeAction((WorkflowActionNode) node, instantiator, classLoader, token);
        break;
      case FORK:
        executeFork(appSpec, (WorkflowForkNode) node, instantiator, classLoader, token);
        break;
      case CONDITION:
        executeCondition(appSpec, (WorkflowConditionNode) node, instantiator, classLoader, token);
        break;
      default:
        break;
    }
  }

  @SuppressWarnings("unchecked")
  private void executeCondition(ApplicationSpecification appSpec, WorkflowConditionNode node,
                                InstantiatorFactory instantiator, ClassLoader classLoader,
                                WorkflowToken token) throws Exception {
    Class<?> clz = classLoader.loadClass(node.getPredicateClassName());
    Predicate<WorkflowContext> predicate = instantiator.get(
      TypeToken.of((Class<? extends Predicate<WorkflowContext>>) clz)).create();

    WorkflowContext context = new BasicWorkflowContext(workflowSpec, null, logicalStartTime, null,
                                                       new BasicArguments(runtimeArgs), token,
                                                       program, runId, metricsCollectionService,
                                                       datasetFramework, txClient, discoveryServiceClient);
    Iterator<WorkflowNode> iterator;
    if (predicate.apply(context)) {
      // execute the if branch
      iterator = node.getIfBranch().iterator();
    } else {
      // execute the else branch
      iterator = node.getElseBranch().iterator();
    }
    // If a workflow updates its token at a condition node, it will be persisted after the execution of the next node.
    // However, the call below ensures that even if the workflow fails/crashes after a condition node, updates from the
    // condition node are also persisted.
    store.updateWorkflowToken(workflowId, runId.getId(), token);
    executeAll(iterator, appSpec, instantiator, classLoader, token);
  }

  private void createLocalDatasets() throws IOException, DatasetManagementException {
    for (Map.Entry<String, String> entry : datasetFramework.getDatasetNameMapping().entrySet()) {
      String localInstanceName = entry.getValue();
      Id.DatasetInstance instanceId = Id.DatasetInstance.from(program.getNamespaceId(), localInstanceName);
      DatasetCreationSpec instanceSpec = workflowSpec.getLocalDatasetSpecs().get(entry.getKey());
      LOG.debug("Adding Workflow local dataset instance: {}", localInstanceName);
      datasetFramework.addInstance(instanceSpec.getTypeName(), instanceId, instanceSpec.getProperties());
    }
  }

  private void deleteLocalDatasets() {
    for (Map.Entry<String, String> entry : datasetFramework.getDatasetNameMapping().entrySet()) {
      Map<String, String> datasetArguments = RuntimeArguments.extractScope(Scope.DATASET, entry.getKey(), runtimeArgs);
      if (Boolean.parseBoolean(datasetArguments.get("keep.local"))) {
        continue;
      }

      String localInstanceName = entry.getValue();
      Id.DatasetInstance instanceId = Id.DatasetInstance.from(program.getNamespaceId(), localInstanceName);
      LOG.debug("Deleting Workflow local dataset instance: {}", localInstanceName);
      try {
        datasetFramework.deleteInstance(instanceId);
      } catch (Throwable t) {
        LOG.warn("Failed to delete the Workflow local dataset instance {}", localInstanceName, t);
      }
    }
  }

  @Override
  protected void run() throws Exception {
    LOG.info("Start workflow execution for {}", workflowSpec);
    WorkflowToken token = new BasicWorkflowToken(cConf.getInt(Constants.AppFabric.WORKFLOW_TOKEN_MAX_SIZE_MB));
    executeAll(workflowSpec.getNodes().iterator(), program.getApplicationSpecification(),
               new InstantiatorFactory(false), program.getClassLoader(), token);

    LOG.info("Workflow execution succeeded for {}", workflowSpec);
  }

  private void executeAll(Iterator<WorkflowNode> iterator, ApplicationSpecification appSpec,
                          InstantiatorFactory instantiator, ClassLoader classLoader, WorkflowToken token) {
    while (iterator.hasNext() && runningThread != null) {
      try {
        blockIfSuspended();
        WorkflowNode node = iterator.next();
        executeNode(appSpec, node, instantiator, classLoader, token);
      } catch (Throwable t) {
        Throwable rootCause = Throwables.getRootCause(t);
        if (rootCause instanceof InterruptedException) {
          LOG.error("Workflow execution aborted.", rootCause);
          break;
        }
        throw Throwables.propagate(rootCause);
      }
    }
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

  private BasicWorkflowContext createWorkflowContext(WorkflowActionSpecification actionSpec,
                                                     WorkflowToken token, String nodeId) {
    return new BasicWorkflowContext(workflowSpec, actionSpec, logicalStartTime,
                                    workflowProgramRunnerFactory.getProgramWorkflowRunner(actionSpec, token, nodeId),
                                    new BasicArguments(runtimeArgs), token, program, runId, metricsCollectionService,
                                    datasetFramework, txClient, discoveryServiceClient);
  }

  /**
   * Instantiates and initialize a WorkflowAction.
   */
  @SuppressWarnings("unchecked")
  private WorkflowAction initialize(WorkflowActionSpecification actionSpec,
                                    ClassLoader classLoader, InstantiatorFactory instantiator,
                                    BasicWorkflowContext workflowContext) throws Exception {
    Class<?> clz = Class.forName(actionSpec.getClassName(), true, classLoader);
    Preconditions.checkArgument(WorkflowAction.class.isAssignableFrom(clz), "%s is not a WorkflowAction.", clz);
    WorkflowAction action = instantiator.get(TypeToken.of((Class<? extends WorkflowAction>) clz)).create();

    ClassLoader oldClassLoader = setContextCombinedClassLoader(action);
    try {
      Reflections.visit(action, action.getClass(),
                        new PropertyFieldSetter(actionSpec.getProperties()),
                        new DataSetFieldSetter(workflowContext),
                        new MetricsFieldSetter(workflowContext.getMetrics()));

      initializeInTransaction(action, workflowContext);
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
    ClassLoader oldClassLoader = setContextCombinedClassLoader(action);
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
    return ImmutableMap.<String, String>builder().putAll(args.asMap()).build();
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

  private ClassLoader setContextCombinedClassLoader(WorkflowAction action) {
    return ClassLoaders.setContextClassLoader(
      new CombineClassLoader(null, ImmutableList.of(action.getClass().getClassLoader(), getClass().getClassLoader())));
  }

  /**
   * Creates an {@link ExecutorService} that has the given number of threads.
   *
   * @param threads number of core threads in the executor
   * @param terminationLatch a {@link CountDownLatch} that will be counted down when the executor terminated
   * @param threadNameFormat name format for the {@link ThreadFactory} provided to the executor
   * @return a new {@link ExecutorService}.
   */
  private ExecutorService createExecutor(int threads, final CountDownLatch terminationLatch, String threadNameFormat) {
    return new ThreadPoolExecutor(
      threads, threads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
      new ThreadFactoryBuilder().setNameFormat(threadNameFormat).build()) {
      @Override
      protected void terminated() {
        terminationLatch.countDown();
      }
    };
  }
}
