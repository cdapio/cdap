/*
 * Copyright © 2014-2019 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.workflow;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.cdap.cdap.api.Predicate;
import io.cdap.cdap.api.ProgramLifecycle;
import io.cdap.cdap.api.ProgramState;
import io.cdap.cdap.api.ProgramStatus;
import io.cdap.cdap.api.annotation.TransactionControl;
import io.cdap.cdap.api.app.ApplicationSpecification;
import io.cdap.cdap.api.common.RuntimeArguments;
import io.cdap.cdap.api.common.Scope;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.metadata.MetadataReader;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.api.schedule.SchedulableProgramType;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreManager;
import io.cdap.cdap.api.workflow.AbstractCondition;
import io.cdap.cdap.api.workflow.Condition;
import io.cdap.cdap.api.workflow.NodeStatus;
import io.cdap.cdap.api.workflow.Workflow;
import io.cdap.cdap.api.workflow.WorkflowActionNode;
import io.cdap.cdap.api.workflow.WorkflowConditionNode;
import io.cdap.cdap.api.workflow.WorkflowContext;
import io.cdap.cdap.api.workflow.WorkflowForkNode;
import io.cdap.cdap.api.workflow.WorkflowNode;
import io.cdap.cdap.api.workflow.WorkflowNodeState;
import io.cdap.cdap.api.workflow.WorkflowNodeType;
import io.cdap.cdap.api.workflow.WorkflowSpecification;
import io.cdap.cdap.api.workflow.WorkflowToken;
import io.cdap.cdap.app.program.Program;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.app.runtime.ProgramRunnerFactory;
import io.cdap.cdap.app.runtime.ProgramStateWriter;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.common.lang.Exceptions;
import io.cdap.cdap.common.lang.InstantiatorFactory;
import io.cdap.cdap.common.lang.PropertyFieldSetter;
import io.cdap.cdap.common.logging.LoggingContextAccessor;
import io.cdap.cdap.common.namespace.NamespaceQueryAdmin;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.data2.dataset2.DatasetFramework;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.MetadataPublisher;
import io.cdap.cdap.data2.transaction.Transactions;
import io.cdap.cdap.internal.app.runtime.AbstractContext;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.DataSetFieldSetter;
import io.cdap.cdap.internal.app.runtime.MetricsFieldSetter;
import io.cdap.cdap.internal.app.runtime.ProgramRunners;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.customaction.BasicCustomActionContext;
import io.cdap.cdap.internal.app.runtime.plugin.PluginInstantiator;
import io.cdap.cdap.internal.dataset.DatasetCreationSpec;
import io.cdap.cdap.internal.lang.Reflections;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.messaging.MessagingService;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.WorkflowNodeStateDetail;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.KerberosPrincipalId;
import io.cdap.cdap.proto.id.ProgramRunId;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

/**
 * Core of Workflow engine that drives the execution of Workflow.
 */
final class WorkflowDriver extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowDriver.class);
  private static final String ACTION_SCOPE = "action";

  private final Program program;
  private final ProgramOptions programOptions;
  private final WorkflowSpecification workflowSpec;
  private final CConfiguration cConf;
  private final ProgramWorkflowRunnerFactory workflowProgramRunnerFactory;
  private final Map<String, WorkflowActionNode> status = new ConcurrentHashMap<>();
  private final Lock lock;
  private final java.util.concurrent.locks.Condition condition;
  private final MetricsCollectionService metricsCollectionService;
  private final NameMappedDatasetFramework datasetFramework;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final TransactionSystemClient txClient;
  private final WorkflowStateWriter workflowStateWriter;
  private final ProgramRunId workflowRunId;
  private final BasicWorkflowContext workflowContext;
  private final BasicWorkflowToken basicWorkflowToken;
  private final Map<String, WorkflowNodeState> nodeStates = new ConcurrentHashMap<>();
  @Nullable
  private final PluginInstantiator pluginInstantiator;
  private final SecureStore secureStore;
  private final SecureStoreManager secureStoreManager;
  private final MessagingService messagingService;
  private final MetadataReader metadataReader;
  private final MetadataPublisher metadataPublisher;
  private final FieldLineageWriter fieldLineageWriter;
  private final NamespaceQueryAdmin namespaceQueryAdmin;
  private final RemoteClientFactory remoteClientFactory;

  private volatile Thread runningThread;
  private boolean suspended;
  private Workflow workflow;

  WorkflowDriver(Program program, ProgramOptions options,
                 WorkflowSpecification workflowSpec, ProgramRunnerFactory programRunnerFactory,
                 MetricsCollectionService metricsCollectionService,
                 DatasetFramework datasetFramework, DiscoveryServiceClient discoveryServiceClient,
                 TransactionSystemClient txClient, WorkflowStateWriter workflowStateWriter, CConfiguration cConf,
                 @Nullable PluginInstantiator pluginInstantiator, SecureStore secureStore,
                 SecureStoreManager secureStoreManager, MessagingService messagingService,
                 ProgramStateWriter programStateWriter, MetadataReader metadataReader,
                 MetadataPublisher metadataPublisher, FieldLineageWriter fieldLineageWriter,
                 NamespaceQueryAdmin namespaceQueryAdmin, RemoteClientFactory remoteClientFactory) {
    this.program = program;
    this.programOptions = options;
    this.workflowSpec = workflowSpec;
    this.cConf = cConf;
    this.lock = new ReentrantLock();
    this.condition = lock.newCondition();
    this.metricsCollectionService = metricsCollectionService;
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
    this.workflowStateWriter = workflowStateWriter;
    this.workflowProgramRunnerFactory = new ProgramWorkflowRunnerFactory(cConf, workflowSpec, programRunnerFactory,
                                                                         program, options, programStateWriter);

    this.workflowRunId = program.getId().run(ProgramRunners.getRunId(options));
    this.datasetFramework = new NameMappedDatasetFramework(datasetFramework,
                                                           workflowSpec.getLocalDatasetSpecs().keySet(),
                                                           workflowRunId.getRun());
    this.basicWorkflowToken = new BasicWorkflowToken(cConf.getInt(Constants.AppFabric.WORKFLOW_TOKEN_MAX_SIZE_MB));
    this.workflowContext = new BasicWorkflowContext(workflowSpec,
                                                    basicWorkflowToken, program, programOptions, cConf,
                                                    metricsCollectionService, this.datasetFramework, txClient,
                                                    discoveryServiceClient, nodeStates, pluginInstantiator,
                                                    secureStore, secureStoreManager, messagingService, null,
                                                    metadataReader, metadataPublisher, namespaceQueryAdmin,
                                                    fieldLineageWriter, remoteClientFactory);
    this.pluginInstantiator = pluginInstantiator;
    this.secureStore = secureStore;
    this.secureStoreManager = secureStoreManager;
    this.messagingService = messagingService;
    this.metadataReader = metadataReader;
    this.metadataPublisher = metadataPublisher;
    this.fieldLineageWriter = fieldLineageWriter;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
    this.remoteClientFactory = remoteClientFactory;
  }

  @Override
  protected void startUp() throws Exception {
    LoggingContextAccessor.setLoggingContext(
      LoggingContextHelper.getLoggingContextWithRunId(workflowRunId, programOptions.getArguments().asMap()));
    runningThread = Thread.currentThread();
    createLocalDatasets();
    workflow = initializeWorkflow();
  }

  @SuppressWarnings("unchecked")
  private Workflow initializeWorkflow() throws Exception {
    Class<?> clz = Class.forName(workflowSpec.getClassName(), true, program.getClassLoader());
    if (!Workflow.class.isAssignableFrom(clz)) {
      throw new IllegalStateException(String.format("%s is not Workflow.", clz));
    }
    Class<? extends Workflow> workflowClass = (Class<? extends Workflow>) clz;
    final Workflow workflow = new InstantiatorFactory(false).get(TypeToken.of(workflowClass)).create();
    // set metrics
    Reflections.visit(workflow, workflow.getClass(), new MetricsFieldSetter(workflowContext.getMetrics()));
    if (!(workflow instanceof ProgramLifecycle)) {
      return workflow;
    }
    final TransactionControl txControl =
      Transactions.getTransactionControl(workflowContext.getDefaultTxControl(), Workflow.class,
                                         workflow, "initialize", WorkflowContext.class);
    basicWorkflowToken.setCurrentNode(workflowSpec.getName());
    workflowContext.setState(new ProgramState(ProgramStatus.INITIALIZING, null));
    workflowContext.initializeProgram((ProgramLifecycle) workflow, txControl, false);
    workflowStateWriter.setWorkflowToken(workflowRunId, basicWorkflowToken);
    return workflow;
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
    // Clear the interrupt flag.
    boolean interrupted = Thread.interrupted();
    try {
      destroyWorkflow();
      deleteLocalDatasets();
      if (pluginInstantiator != null) {
        pluginInstantiator.close();
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void destroyWorkflow() {
    if (!(workflow instanceof ProgramLifecycle)) {
      return;
    }
    final TransactionControl txControl = Transactions.getTransactionControl(workflowContext.getDefaultTxControl(),
                                                                            Workflow.class, workflow, "destroy");
    basicWorkflowToken.setCurrentNode(workflowSpec.getName());
    workflowContext.destroyProgram((ProgramLifecycle) workflow, txControl, false);
    try {
      workflowStateWriter.setWorkflowToken(workflowRunId, basicWorkflowToken);
    } catch (Throwable t) {
      LOG.error("Failed to store the final workflow token of Workflow {}", workflowRunId, t);
    }

    if (ProgramStatus.COMPLETED != workflowContext.getState().getStatus()) {
      return;
    }

    writeFieldLineage(workflowContext);
  }

  private void executeAction(WorkflowActionNode node, WorkflowToken token) throws Exception {
    status.put(node.getNodeId(), node);
    CountDownLatch executorTerminateLatch = new CountDownLatch(1);
    ExecutorService executorService = createExecutor(1, executorTerminateLatch, "action-" + node.getNodeId() + "-%d");

    try {
      // Run the action in new thread
      Future<?> future = executorService.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          SchedulableProgramType programType = node.getProgram().getProgramType();
          String programName = node.getProgram().getProgramName();
          String prettyProgramType = ProgramType.valueOf(programType.name()).getPrettyName();
          ProgramWorkflowRunner programWorkflowRunner =
            workflowProgramRunnerFactory.getProgramWorkflowRunner(programType, token, node.getNodeId(), nodeStates);

          // this should not happen, since null is only passed in from WorkflowDriver, only when calling configure
          if (programWorkflowRunner == null) {
            throw new UnsupportedOperationException("Operation not allowed.");
          }

          Runnable programRunner = programWorkflowRunner.create(programName);
          LOG.info("Starting {} Program '{}' in workflow", prettyProgramType, programName);
          programRunner.run();

          LOG.info("{} Program '{}' in workflow completed", prettyProgramType, programName);
          return null;
        }
      });
      future.get();
    } catch (Throwable t) {
      Throwables.propagateIfPossible(t, Exception.class);
      throw Throwables.propagate(t);
    } finally {
      executorService.shutdownNow();
      executorTerminateLatch.await();
      status.remove(node.getNodeId());
    }
    workflowStateWriter.setWorkflowToken(workflowRunId, token);
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
          ((BasicWorkflowToken) token).mergeToken(branchToken);
          LOG.trace("Execution of branch {} for fork {} completed.", branchInfo, fork);
        } catch (InterruptedException e) {
          // Due to workflow abortion, so just break the loop
          break;
        } catch (ExecutionException e) {
          // Unwrap the cause
          Throwables.propagateIfPossible(e.getCause(), Exception.class);
          throw Throwables.propagate(e.getCause());
        }
      }
    } finally {
      // Update the WorkflowToken after the execution of the FORK node completes.
      workflowStateWriter.setWorkflowToken(workflowRunId, token);
      executorService.shutdownNow();
      // Wait for the executor termination
      executorTerminateLatch.await();
    }
  }

  private void executeCustomAction(final WorkflowActionNode node, InstantiatorFactory instantiator,
                                   final ClassLoader classLoader, WorkflowToken token)  throws Exception {

    CustomActionExecutor customActionExecutor;
    // Node has CustomActionSpecification, so it must represent the CustomAction added in 3.5.0
    // Create instance of the CustomActionExecutor using CustomActionContext

    WorkflowProgramInfo info = new WorkflowProgramInfo(workflowSpec.getName(), node.getNodeId(),
                                                       workflowRunId.getRun(), node.getNodeId(),
                                                       (BasicWorkflowToken) token,
                                                       workflowContext.fieldLineageConsolidationEnabled());
    ProgramOptions actionOptions =
      new SimpleProgramOptions(programOptions.getProgramId(),
                               programOptions.getArguments(),
                               new BasicArguments(RuntimeArguments.extractScope(
                                 ACTION_SCOPE, node.getNodeId(), programOptions.getUserArguments().asMap())));

    BasicCustomActionContext context = new BasicCustomActionContext(program, actionOptions, cConf,
                                                                    node.getCustomActionSpecification(), info,
                                                                    metricsCollectionService, datasetFramework,
                                                                    txClient, discoveryServiceClient,
                                                                    pluginInstantiator, secureStore,
                                                                    secureStoreManager, messagingService,
                                                                    metadataReader, metadataPublisher,
                                                                    namespaceQueryAdmin, fieldLineageWriter,
                                                                    remoteClientFactory);
    customActionExecutor = new CustomActionExecutor(context, instantiator, classLoader);
    status.put(node.getNodeId(), node);
    workflowStateWriter.addWorkflowNodeState(workflowRunId,
                                             new WorkflowNodeStateDetail(node.getNodeId(), NodeStatus.RUNNING));
    Throwable failureCause = null;
    try {
      customActionExecutor.execute();
    } catch (Throwable t) {
      failureCause = t;
      throw t;
    } finally {
      status.remove(node.getNodeId());
      workflowStateWriter.setWorkflowToken(workflowRunId, token);
      NodeStatus status = failureCause == null ? NodeStatus.COMPLETED : NodeStatus.FAILED;
      if (failureCause == null) {
        writeFieldLineage(context);
      }
      nodeStates.put(node.getNodeId(), new WorkflowNodeState(node.getNodeId(), status, null, failureCause));
      BasicThrowable defaultThrowable = failureCause == null ? null : new BasicThrowable(failureCause);
      workflowStateWriter.addWorkflowNodeState(workflowRunId,
                                               new WorkflowNodeStateDetail(node.getNodeId(), status, null,
                                                                           defaultThrowable));
    }
  }

  private void writeFieldLineage(AbstractContext context) {
    try {
      if (!context.getFieldLineageOperations().isEmpty()) {
        context.flushLineage();
      }
    } catch (Throwable t) {
      LOG.debug("Failed to emit the field lineage operations for Workflow {}", workflowRunId, t);
    }
  }

  private void executeNode(ApplicationSpecification appSpec, WorkflowNode node, InstantiatorFactory instantiator,
                           ClassLoader classLoader, WorkflowToken token) throws Exception {
    WorkflowNodeType nodeType = node.getType();
    ((BasicWorkflowToken) token).setCurrentNode(node.getNodeId());
    switch (nodeType) {
      case ACTION:
        WorkflowActionNode actionNode = (WorkflowActionNode) node;
        if (SchedulableProgramType.CUSTOM_ACTION == actionNode.getProgram().getProgramType()) {
          executeCustomAction(actionNode, instantiator, classLoader, token);
        } else {
          executeAction(actionNode, token);
        }
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
  private void executeCondition(ApplicationSpecification appSpec, final WorkflowConditionNode node,
                                InstantiatorFactory instantiator, ClassLoader classLoader,
                                WorkflowToken token) throws Exception {

    final BasicWorkflowContext context = new BasicWorkflowContext(workflowSpec, token, program, programOptions,
                                                                  cConf, metricsCollectionService, datasetFramework,
                                                                  txClient, discoveryServiceClient, nodeStates,
                                                                  pluginInstantiator, secureStore, secureStoreManager,
                                                                  messagingService, node.getConditionSpecification(),
                                                                  metadataReader, metadataPublisher,
                                                                  namespaceQueryAdmin, fieldLineageWriter,
                                                                  remoteClientFactory);
    final Iterator<WorkflowNode> iterator;
    Class<?> clz = classLoader.loadClass(node.getPredicateClassName());
    Predicate<WorkflowContext> predicate = instantiator.get(
      TypeToken.of((Class<? extends Predicate<WorkflowContext>>) clz)).create();

    if (!(predicate instanceof Condition)) {
      iterator = predicate.apply(context) ? node.getIfBranch().iterator() : node.getElseBranch().iterator();
    } else {
      final Condition workflowCondition = (Condition) predicate;

      Reflections.visit(workflowCondition, workflowCondition.getClass(),
                        new PropertyFieldSetter(node.getConditionSpecification().getProperties()),
                        new DataSetFieldSetter(context), new MetricsFieldSetter(context.getMetrics()));

      TransactionControl defaultTxControl = workflowContext.getDefaultTxControl();
      try {
        // AbstractCondition implements final initialize(context) and requires subclass to
        // implement initialize(), whereas conditions that directly implement Condition can
        // override initialize(context)
        TransactionControl txControl = workflowCondition instanceof AbstractCondition
          ? Transactions.getTransactionControl(defaultTxControl, AbstractCondition.class,
                                               workflowCondition, "initialize")
          : Transactions.getTransactionControl(defaultTxControl, Condition.class,
                                               workflowCondition, "initialize", WorkflowContext.class);

        context.initializeProgram(workflowCondition, txControl, false);
        boolean result = context.execute(() -> workflowCondition.apply(context));
        iterator = result ? node.getIfBranch().iterator() : node.getElseBranch().iterator();
      } finally {
        TransactionControl txControl = Transactions.getTransactionControl(defaultTxControl, Condition.class,
                                                                          workflowCondition, "destroy");
        context.destroyProgram(workflowCondition, txControl, false);
      }
    }

    // If a workflow updates its token at a condition node, it will be persisted after the execution of the next node.
    // However, the call below ensures that even if the workflow fails/crashes after a condition node, updates from the
    // condition node are also persisted.
    workflowStateWriter.setWorkflowToken(workflowRunId, token);
    executeAll(iterator, appSpec, instantiator, classLoader, token);
  }

  private DatasetProperties addLocalDatasetProperty(DatasetProperties properties, boolean keepLocal) {
    String dsDescription = properties.getDescription();
    DatasetProperties.Builder builder = DatasetProperties.builder();
    builder.addAll(properties.getProperties());
    builder.add(Constants.AppFabric.WORKFLOW_LOCAL_DATASET_PROPERTY, "true");
    builder.add(Constants.AppFabric.WORKFLOW_NAMESPACE_NAME, workflowRunId.getNamespace());
    builder.add(Constants.AppFabric.WORKFLOW_APPLICATION_NAME, workflowRunId.getApplication());
    builder.add(Constants.AppFabric.WORKFLOW_APPLICATION_VERSION, workflowRunId.getVersion());
    builder.add(Constants.AppFabric.WORKFLOW_PROGRAM_NAME, workflowRunId.getProgram());
    builder.add(Constants.AppFabric.WORKFLOW_RUN_ID, workflowRunId.getRun());
    if (keepLocal) {
      builder.add(Constants.AppFabric.WORKFLOW_KEEP_LOCAL, "true");
    }
    builder.setDescription(dsDescription);
    return builder.build();
  }

  private boolean keepLocal(String originalDatasetName) {
    Map<String, String> datasetArguments = RuntimeArguments.extractScope(Scope.DATASET, originalDatasetName,
                                                                         workflowContext.getRuntimeArguments());

    return Boolean.parseBoolean(datasetArguments.get("keep.local"));
  }

  private void createLocalDatasets() throws IOException, DatasetManagementException {
    final KerberosPrincipalId principalId = ProgramRunners.getApplicationPrincipal(programOptions);

    for (final Map.Entry<String, String> entry : datasetFramework.getDatasetNameMapping().entrySet()) {
      final String localInstanceName = entry.getValue();
      final DatasetId instanceId = new DatasetId(workflowRunId.getNamespace(), localInstanceName);
      final DatasetCreationSpec instanceSpec = workflowSpec.getLocalDatasetSpecs().get(entry.getKey());
      LOG.debug("Adding Workflow local dataset instance: {}", localInstanceName);

      try {
        Retries.callWithRetries(new Retries.Callable<Void, Exception>() {
          @Override
          public Void call() throws Exception {
            DatasetProperties properties = addLocalDatasetProperty(instanceSpec.getProperties(),
                                                                   keepLocal(entry.getKey()));
            // we have to do this check since addInstance method can only be used when app impersonation is enabled
            if (principalId != null) {
              datasetFramework.addInstance(instanceSpec.getTypeName(), instanceId, properties, principalId);
            } else {
              datasetFramework.addInstance(instanceSpec.getTypeName(), instanceId, properties);
            }
            return null;
          }
        }, RetryStrategies.fixDelay(Constants.Retry.LOCAL_DATASET_OPERATION_RETRY_DELAY_SECONDS, TimeUnit.SECONDS));
      } catch (IOException | DatasetManagementException e) {
        throw e;
      } catch (Exception e) {
        // this should never happen
        throw new IllegalStateException(e);
      }
    }
  }

  private void deleteLocalDatasets() {
    for (final Map.Entry<String, String> entry : datasetFramework.getDatasetNameMapping().entrySet()) {
      if (keepLocal(entry.getKey())) {
        continue;
      }

      final String localInstanceName = entry.getValue();
      final DatasetId instanceId = new DatasetId(workflowRunId.getNamespace(), localInstanceName);
      LOG.debug("Deleting Workflow local dataset instance: {}", localInstanceName);

      try {
        Retries.runWithRetries(() -> datasetFramework.deleteInstance(instanceId),
                               RetryStrategies.fixDelay(Constants.Retry.LOCAL_DATASET_OPERATION_RETRY_DELAY_SECONDS,
                                                        TimeUnit.SECONDS));
      } catch (Exception e) {
        LOG.warn("Failed to delete the Workflow local dataset instance {}", localInstanceName, e);
      }
    }
  }

  @Override
  protected void run() throws Exception {
    LOG.info("Starting workflow execution for '{}' with Run id '{}'", workflowSpec.getName(), workflowRunId.getRun());
    LOG.trace("Workflow specification is {}", workflowSpec);
    workflowContext.setState(new ProgramState(ProgramStatus.RUNNING, null));
    executeAll(workflowSpec.getNodes().iterator(), program.getApplicationSpecification(),
               new InstantiatorFactory(false), program.getClassLoader(), basicWorkflowToken);
    if (runningThread != null) {
      workflowContext.setState(new ProgramState(ProgramStatus.COMPLETED, null));
    }
    LOG.info("Workflow '{}' with run id '{}' completed", workflowSpec.getName(), workflowRunId.getRun());
  }

  private void executeAll(Iterator<WorkflowNode> iterator, ApplicationSpecification appSpec,
                          InstantiatorFactory instantiator, ClassLoader classLoader, WorkflowToken token)
    throws Exception {
    while (iterator.hasNext() && runningThread != null) {
      try {
        blockIfSuspended();
        WorkflowNode node = iterator.next();
        executeNode(appSpec, node, instantiator, classLoader, token);
      } catch (Throwable t) {
        Throwable rootCause = Throwables.getRootCause(t);
        if (rootCause instanceof InterruptedException) {
          LOG.debug("Workflow '{}' with run id '{}' aborted", workflowSpec.getName(),
                    workflowRunId.getRun());
          workflowContext.setState(new ProgramState(ProgramStatus.KILLED, rootCause.getMessage()));
          break;
        }
        workflowContext.setState(new ProgramState(ProgramStatus.FAILED, Exceptions.condenseThrowableMessage(t)));
        throw t;
      }
    }
  }

  @Override
  protected void triggerShutdown() {
    Thread t = runningThread;
    runningThread = null;
    t.interrupt();
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
