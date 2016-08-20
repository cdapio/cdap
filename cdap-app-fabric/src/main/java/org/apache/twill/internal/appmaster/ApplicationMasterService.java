/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.twill.internal.appmaster;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.DiscreteDomains;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multiset;
import com.google.common.collect.Ranges;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Records;
import org.apache.twill.api.Command;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.EventHandlerSpecification;
import org.apache.twill.api.LocalFile;
import org.apache.twill.api.ResourceReport;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.Configs;
import org.apache.twill.internal.Constants;
import org.apache.twill.internal.ContainerInfo;
import org.apache.twill.internal.DefaultTwillRunResources;
import org.apache.twill.internal.EnvKeys;
import org.apache.twill.internal.JvmOptions;
import org.apache.twill.internal.ProcessLauncher;
import org.apache.twill.internal.TwillContainerLauncher;
import org.apache.twill.internal.json.JvmOptionsCodec;
import org.apache.twill.internal.json.LocalFileCodec;
import org.apache.twill.internal.json.TwillSpecificationAdapter;
import org.apache.twill.internal.state.Message;
import org.apache.twill.internal.utils.Instances;
import org.apache.twill.internal.yarn.AbstractYarnTwillService;
import org.apache.twill.internal.yarn.YarnAMClient;
import org.apache.twill.internal.yarn.YarnContainerInfo;
import org.apache.twill.internal.yarn.YarnContainerStatus;
import org.apache.twill.internal.yarn.YarnUtils;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * The class that acts as {@code ApplicationMaster} for Twill applications.
 */
public final class ApplicationMasterService extends AbstractYarnTwillService implements Supplier<ResourceReport> {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationMasterService.class);
  private static final Gson GSON = new Gson();

  // Copied from org.apache.hadoop.yarn.security.AMRMTokenIdentifier.KIND_NAME since it's missing in Hadoop-2.0
  private static final Text AMRM_TOKEN_KIND_NAME = new Text("YARN_AM_RM_TOKEN");

  private final RunId runId;
  private final ZKClient zkClient;
  private final TwillSpecification twillSpec;
  private final ApplicationMasterLiveNodeData amLiveNode;
  private final RunningContainers runningContainers;
  private final ExpectedContainers expectedContainers;
  private final YarnAMClient amClient;
  private final JvmOptions jvmOpts;
  private final int reservedMemory;
  private final EventHandler eventHandler;
  private final Location applicationLocation;
  private final PlacementPolicyManager placementPolicyManager;

  private volatile boolean stopped;
  private Queue<RunnableContainerRequest> runnableContainerRequests;
  private Set<String> restartingRunnables; // Contains set of runnables that are being restarted
  private ExecutorService instanceChangeExecutor;

  public ApplicationMasterService(RunId runId, ZKClient zkClient, File twillSpecFile,
                                  YarnAMClient amClient, Location applicationLocation) throws Exception {
    super(zkClient, runId, applicationLocation);

    this.runId = runId;
    this.twillSpec = TwillSpecificationAdapter.create().fromJson(twillSpecFile);
    this.zkClient = zkClient;
    this.applicationLocation = applicationLocation;
    this.amClient = amClient;
    this.credentials = createCredentials();
    this.jvmOpts = loadJvmOptions();
    this.reservedMemory = getReservedMemory();
    this.placementPolicyManager = new PlacementPolicyManager(twillSpec.getPlacementPolicies());

    this.amLiveNode = new ApplicationMasterLiveNodeData(Integer.parseInt(System.getenv(EnvKeys.YARN_APP_ID)),
                                                        Long.parseLong(System.getenv(EnvKeys.YARN_APP_ID_CLUSTER_TIME)),
                                                        amClient.getContainerId().toString());

    expectedContainers = initExpectedContainers(twillSpec);
    runningContainers = initRunningContainers(amClient.getContainerId(), amClient.getHost());
    eventHandler = createEventHandler(twillSpec);
    restartingRunnables = new ConcurrentSkipListSet<>();
  }

  private JvmOptions loadJvmOptions() throws IOException {
    final File jvmOptsFile = new File(Constants.Files.JVM_OPTIONS);
    if (!jvmOptsFile.exists()) {
      return new JvmOptions(null, JvmOptions.DebugOptions.NO_DEBUG);
    }
    return JvmOptionsCodec.decode(new InputSupplier<Reader>() {
      @Override
      public Reader getInput() throws IOException {
        return new FileReader(jvmOptsFile);
      }
    });
  }

  private int getReservedMemory() {
    String value = System.getenv(EnvKeys.TWILL_RESERVED_MEMORY_MB);
    if (value == null) {
      return Configs.Defaults.JAVA_RESERVED_MEMORY_MB;
    }
    try {
      return Integer.parseInt(value);
    } catch (Exception e) {
      return Configs.Defaults.JAVA_RESERVED_MEMORY_MB;
    }
  }

  @SuppressWarnings("unchecked")
  private EventHandler createEventHandler(TwillSpecification twillSpec) {
    try {
      // Should be able to load by this class ClassLoader, as they packaged in the same jar.
      EventHandlerSpecification handlerSpec = twillSpec.getEventHandler();

      Class<?> handlerClass = getClass().getClassLoader().loadClass(handlerSpec.getClassName());
      Preconditions.checkArgument(EventHandler.class.isAssignableFrom(handlerClass),
                                  "Class {} does not implements {}",
                                  handlerClass, EventHandler.class.getName());
      return Instances.newInstance((Class<? extends EventHandler>) handlerClass);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private RunningContainers initRunningContainers(ContainerId appMasterContainerId,
                                                  String appMasterHost) throws Exception {
    TwillRunResources appMasterResources = new DefaultTwillRunResources(
      0,
      appMasterContainerId.toString(),
      Integer.parseInt(System.getenv(EnvKeys.YARN_CONTAINER_VIRTUAL_CORES)),
      Integer.parseInt(System.getenv(EnvKeys.YARN_CONTAINER_MEMORY_MB)),
      appMasterHost, null, null);
    String appId = appMasterContainerId.getApplicationAttemptId().getApplicationId().toString();
    return new RunningContainers(appId, appMasterResources, zkClient);
  }

  private ExpectedContainers initExpectedContainers(TwillSpecification twillSpec) {
    Map<String, Integer> expectedCounts = Maps.newHashMap();
    for (RuntimeSpecification runtimeSpec : twillSpec.getRunnables().values()) {
      expectedCounts.put(runtimeSpec.getName(), runtimeSpec.getResourceSpecification().getInstances());
    }
    return new ExpectedContainers(expectedCounts);
  }

  @Override
  public ResourceReport get() {
    return runningContainers.getResourceReport();
  }

  @Override
  protected void doStart() throws Exception {
    LOG.info("Start application master with spec: " + TwillSpecificationAdapter.create().toJson(twillSpec));

    // initialize the event handler, if it fails, it will fail the application.
    eventHandler.initialize(new BasicEventHandlerContext(twillSpec.getEventHandler()));

    instanceChangeExecutor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("instanceChanger"));

    // Creates ZK path for runnable
    zkClient.create("/" + runId.getId() + "/runnables", null, CreateMode.PERSISTENT).get();
    runningContainers.addWatcher("/discoverable");
    runnableContainerRequests = initContainerRequests();
  }

  @Override
  protected void doStop() throws Exception {
    Thread.interrupted();     // This is just to clear the interrupt flag

    LOG.info("Stop application master with spec: {}", TwillSpecificationAdapter.create().toJson(twillSpec));

    try {
      // call event handler destroy. If there is error, only log and not affected stop sequence.
      eventHandler.destroy();
    } catch (Throwable t) {
      LOG.warn("Exception when calling {}.destroy()", twillSpec.getEventHandler().getClassName(), t);
    }

    instanceChangeExecutor.shutdownNow();

    // For checking if all containers are stopped.
    final Set<String> ids = Sets.newHashSet(runningContainers.getContainerIds());
    final YarnAMClient.AllocateHandler handler = new YarnAMClient.AllocateHandler() {
      @Override
      public void acquired(List<? extends ProcessLauncher<YarnContainerInfo>> launchers) {
        // no-op
      }

      @Override
      public void completed(List<YarnContainerStatus> completed) {
        for (YarnContainerStatus status : completed) {
          handleCompleted(completed);
          ids.remove(status.getContainerId());
        }
      }
    };

    // Handle heartbeats during shutdown because runningContainers.stopAll() waits until
    // handleCompleted() is called for every stopped runnable
    ExecutorService stopPoller = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("stopPoller"));
    stopPoller.execute(new Runnable() {
      @Override
      public void run() {
        while (!ids.isEmpty()) {
          try {
            amClient.allocate(0.0f, handler);
            TimeUnit.SECONDS.sleep(1);
          } catch (Exception e) {
            LOG.error("Got exception while getting heartbeat", e);
          }
        }
      }
    });

    // runningContainers.stopAll() will wait for all the running runnables to stop or kill them after a timeout
    runningContainers.stopAll();
    // Since all the runnables are now stopped, it is okay to stop the poller.
    stopPoller.shutdownNow();
    cleanupDir();
  }

  @Override
  protected Object getLiveNodeData() {
    return amLiveNode;
  }

  @Override
  public ListenableFuture<String> onReceived(String messageId, Message message) {
    LOG.debug("Message received: {} {}.", messageId, message);

    SettableFuture<String> result = SettableFuture.create();
    Runnable completion = getMessageCompletion(messageId, result);

    if (handleSecureStoreUpdate(message)) {
      runningContainers.sendToAll(message, completion);
      return result;
    }

    if (handleSetInstances(message, completion)) {
      return result;
    }

    if (handleRestartRunnablesInstances(message, completion)) {
      return result;
    }

    // Replicate messages to all runnables
    if (message.getScope() == Message.Scope.ALL_RUNNABLE) {
      runningContainers.sendToAll(message, completion);
      return result;
    }

    // Replicate message to a particular runnable.
    if (message.getScope() == Message.Scope.RUNNABLE) {
      runningContainers.sendToRunnable(message.getRunnableName(), message, completion);
      return result;
    }

    LOG.info("Message ignored. {}", message);
    return Futures.immediateFuture(messageId);
  }

  @Override
  protected void triggerShutdown() {
    stopped = true;
  }

  private void cleanupDir() {
    try {
      if (applicationLocation.delete(true)) {
        LOG.info("Application directory deleted: {}", applicationLocation);
      } else {
        LOG.warn("Failed to cleanup directory {}.", applicationLocation);
      }
    } catch (Exception e) {
      LOG.warn("Exception while cleanup directory {}.", applicationLocation, e);
    }
  }

  @Override
  protected void doRun() throws Exception {
    // The main loop
    Map.Entry<AllocationSpecification, ? extends Collection<RuntimeSpecification>> currentRequest = null;
    final Queue<ProvisionRequest> provisioning = Lists.newLinkedList();

    YarnAMClient.AllocateHandler allocateHandler = new YarnAMClient.AllocateHandler() {
      @Override
      public void acquired(List<? extends ProcessLauncher<YarnContainerInfo>> launchers) {
        launchRunnable(launchers, provisioning);
      }

      @Override
      public void completed(List<YarnContainerStatus> completed) {
        handleCompleted(completed);
      }
    };

    long requestStartTime = 0;
    boolean isRequestRelaxed = false;
    long nextTimeoutCheck = System.currentTimeMillis() + Constants.PROVISION_TIMEOUT;
    while (!stopped) {
      // Call allocate. It has to be made at first in order to be able to get cluster resource availability.
      amClient.allocate(0.0f, allocateHandler);

      // Looks for containers requests.
      if (provisioning.isEmpty() && runnableContainerRequests.isEmpty() && runningContainers.isEmpty() &&
        restartingRunnables.isEmpty()) {
        LOG.info("All containers completed. Shutting down application master.");
        break;
      }

      // If nothing is in provisioning, and no pending request, move to next one
      while (provisioning.isEmpty() && currentRequest == null && !runnableContainerRequests.isEmpty()) {
        currentRequest = runnableContainerRequests.peek().takeRequest();
        if (currentRequest == null) {
          // All different types of resource request from current order is done, move to next one
          // TODO: Need to handle order type as well
          runnableContainerRequests.poll();
        }
      }

      // Nothing in provision, makes the next batch of provision request
      if (provisioning.isEmpty() && currentRequest != null) {
        manageBlacklist(currentRequest);
        addContainerRequests(currentRequest.getKey().getResource(), currentRequest.getValue(), provisioning,
                             currentRequest.getKey().getType());
        currentRequest = null;
        requestStartTime = System.currentTimeMillis();
        isRequestRelaxed = false;
      }

      // Check for provision request timeout i.e. check if any provision request has been pending
      // for more than the designated time. On timeout, relax the request constraints.
      if (!provisioning.isEmpty() && !isRequestRelaxed &&
        (System.currentTimeMillis() - requestStartTime) > Constants.CONSTRAINED_PROVISION_REQUEST_TIMEOUT) {
        LOG.info("Relaxing provisioning constraints for request {}", provisioning.peek().getRequestId());
        // Clear the blacklist for the pending provision request(s).
        amClient.clearBlacklist();
        isRequestRelaxed = true;
      }

      nextTimeoutCheck = checkProvisionTimeout(nextTimeoutCheck);

      if (isRunning()) {
        TimeUnit.SECONDS.sleep(1);
      }
    }
  }

  /**
   * Manage Blacklist for a given request.
   */
  private void manageBlacklist(Map.Entry<AllocationSpecification, ? extends Collection<RuntimeSpecification>> request) {
    amClient.clearBlacklist();

    //Check the allocation strategy
    AllocationSpecification currentAllocationSpecification = request.getKey();
    if (currentAllocationSpecification.getType().equals(AllocationSpecification.Type.ALLOCATE_ONE_INSTANCE_AT_A_TIME)) {

      //Check the placement policy
      TwillSpecification.PlacementPolicy placementPolicy =
        placementPolicyManager.getPlacementPolicy(currentAllocationSpecification.getRunnableName());
      if (placementPolicy.getType().equals(TwillSpecification.PlacementPolicy.Type.DISTRIBUTED)) {

        //Update blacklist with hosts which are running DISTRIBUTED runnables
        for (String runnable : placementPolicyManager.getFellowRunnables(request.getKey().getRunnableName())) {
          Collection<ContainerInfo> containerStats =
            runningContainers.getContainerInfo(runnable);
          for (ContainerInfo containerInfo : containerStats) {
            // Yarn Resource Manager may include port in the node name depending on the setting
            // YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME. It is safe to add both
            // the names (with and without port) to the blacklist.
            amClient.addToBlacklist(containerInfo.getHost().getHostName());
            amClient.addToBlacklist(containerInfo.getHost().getHostName() + ":" + containerInfo.getPort());
          }
        }
      }
    }
  }

  /**
   * Handling containers that are completed.
   */
  private void handleCompleted(List<YarnContainerStatus> completedContainersStatuses) {
    Multiset<String> restartRunnables = HashMultiset.create();
    for (YarnContainerStatus status : completedContainersStatuses) {
      LOG.info("Container {} completed with {}:{}.",
               status.getContainerId(), status.getState(), status.getDiagnostics());
      runningContainers.handleCompleted(status, restartRunnables);
    }

    for (Multiset.Entry<String> entry : restartRunnables.entrySet()) {
      LOG.info("Re-request container for {} with {} instances.", entry.getElement(), entry.getCount());
      for (int i = 0; i < entry.getCount(); i++) {
        runnableContainerRequests.add(createRunnableContainerRequest(entry.getElement()));
      }
    }

    // For all runnables that needs to re-request for containers, update the expected count timestamp
    // so that the EventHandler would triggered with the right expiration timestamp.
    expectedContainers.updateRequestTime(restartRunnables.elementSet());
  }

  /**
   * Check for containers provision timeout and invoke eventHandler if necessary.
   *
   * @return the timestamp for the next time this method needs to be called.
   */
  private long checkProvisionTimeout(long nextTimeoutCheck) {
    if (System.currentTimeMillis() < nextTimeoutCheck) {
      return nextTimeoutCheck;
    }

    // Invoke event handler for provision request timeout
    Map<String, ExpectedContainers.ExpectedCount> expiredRequests = expectedContainers.getAll();
    Map<String, Integer> runningCounts = runningContainers.countAll();
    Map<String, Integer> completedContainerCount = runningContainers.getCompletedContainerCount();

    List<EventHandler.TimeoutEvent> timeoutEvents = Lists.newArrayList();
    for (Map.Entry<String, ExpectedContainers.ExpectedCount> entry : expiredRequests.entrySet()) {
      String runnableName = entry.getKey();
      ExpectedContainers.ExpectedCount expectedCount = entry.getValue();
      int runningCount = runningCounts.containsKey(runnableName) ? runningCounts.get(runnableName) : 0;
      int completedCount = completedContainerCount.containsKey(runnableName) ?
        completedContainerCount.get(runnableName) : 0;
      if (expectedCount.getCount() > runningCount + completedCount) {
        timeoutEvents.add(new EventHandler.TimeoutEvent(runnableName, expectedCount.getCount(),
                                                        runningCount, expectedCount.getTimestamp()));
      }
    }

    if (!timeoutEvents.isEmpty()) {
      try {
        EventHandler.TimeoutAction action = eventHandler.launchTimeout(timeoutEvents);
        if (action.getTimeout() < 0) {
          // Abort application
          stop();
        } else {
          return nextTimeoutCheck + action.getTimeout();
        }
      } catch (Throwable t) {
        LOG.warn("Exception when calling EventHandler {}. Ignore the result.", t);
      }
    }
    return nextTimeoutCheck + Constants.PROVISION_TIMEOUT;
  }

  private Credentials createCredentials() {
    Credentials credentials = new Credentials();
    if (!UserGroupInformation.isSecurityEnabled()) {
      return credentials;
    }

    try {
      credentials.addAll(UserGroupInformation.getCurrentUser().getCredentials());

      // Remove the AM->RM tokens
      Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
      while (iter.hasNext()) {
        Token<?> token = iter.next();
        if (token.getKind().equals(AMRM_TOKEN_KIND_NAME)) {
          iter.remove();
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to get current user. No credentials will be provided to containers.", e);
    }

    return credentials;
  }

  private Queue<RunnableContainerRequest> initContainerRequests() {
    // Orderly stores container requests.
    Queue<RunnableContainerRequest> requests = Lists.newLinkedList();
    // For each order in the twillSpec, create container request for runnables, depending on Placement policy.
    for (TwillSpecification.Order order : twillSpec.getOrders()) {
      Set<String> distributedRunnables = placementPolicyManager.getDistributedRunnables(order.getNames());
      Set<String> defaultRunnables = Sets.newHashSet();
      defaultRunnables.addAll(order.getNames());
      defaultRunnables.removeAll(distributedRunnables);
      Map<AllocationSpecification, Collection<RuntimeSpecification>> requestsMap = Maps.newHashMap();
      for (String runnableName : distributedRunnables) {
        RuntimeSpecification runtimeSpec = twillSpec.getRunnables().get(runnableName);
        Resource capability = createCapability(runtimeSpec.getResourceSpecification());
        for (int instanceId = 0; instanceId < runtimeSpec.getResourceSpecification().getInstances(); instanceId++) {
          AllocationSpecification allocationSpecification =
            new AllocationSpecification(capability, AllocationSpecification.Type.ALLOCATE_ONE_INSTANCE_AT_A_TIME,
                                        runnableName, instanceId);
          addAllocationSpecification(allocationSpecification, requestsMap, runtimeSpec);
        }
      }
      for (String runnableName : defaultRunnables) {
        RuntimeSpecification runtimeSpec = twillSpec.getRunnables().get(runnableName);
        Resource capability = createCapability(runtimeSpec.getResourceSpecification());
        AllocationSpecification allocationSpecification = new AllocationSpecification(capability);
        addAllocationSpecification(allocationSpecification, requestsMap, runtimeSpec);
      }
      requests.add(new RunnableContainerRequest(order.getType(), requestsMap));
    }
    return requests;
  }

  /**
   * Helper method to create {@link org.apache.twill.internal.appmaster.RunnableContainerRequest}.
   */
  private void addAllocationSpecification(AllocationSpecification allocationSpecification,
                                          Map<AllocationSpecification, Collection<RuntimeSpecification>> map,
                                          RuntimeSpecification runtimeSpec) {
    if (!map.containsKey(allocationSpecification)) {
      map.put(allocationSpecification, Lists.<RuntimeSpecification>newLinkedList());
    }
    map.get(allocationSpecification).add(runtimeSpec);
  }

  /**
   * Adds container requests with the given resource capability for each runtime.
   */
  private void addContainerRequests(Resource capability,
                                    Collection<RuntimeSpecification> runtimeSpecs,
                                    Queue<ProvisionRequest> provisioning,
                                    AllocationSpecification.Type allocationType) {
    for (RuntimeSpecification runtimeSpec : runtimeSpecs) {
      String name = runtimeSpec.getName();
      int newContainers = expectedContainers.getExpected(name) - runningContainers.count(name);
      if (newContainers > 0) {
        if (allocationType.equals(AllocationSpecification.Type.ALLOCATE_ONE_INSTANCE_AT_A_TIME)) {
          //Spawning 1 instance at a time
          newContainers = 1;
        }
        TwillSpecification.PlacementPolicy placementPolicy = placementPolicyManager.getPlacementPolicy(name);
        Set<String> hosts = Sets.newHashSet();
        Set<String> racks = Sets.newHashSet();
        if (placementPolicy != null) {
          hosts = placementPolicy.getHosts();
          racks = placementPolicy.getRacks();
        }
        // TODO: Allow user to set priority?
        LOG.info("Request {} container with capability {} for runnable {}", newContainers, capability, name);
        String requestId = amClient.addContainerRequest(capability, newContainers)
          .addHosts(hosts)
          .addRacks(racks)
          .setPriority(0).apply();
        provisioning.add(new ProvisionRequest(runtimeSpec, requestId, newContainers, allocationType));
      }
    }
  }

  /**
   * Launches runnables in the provisioned containers.
   */
  private void launchRunnable(List<? extends ProcessLauncher<YarnContainerInfo>> launchers,
                              Queue<ProvisionRequest> provisioning) {
    for (ProcessLauncher<YarnContainerInfo> processLauncher : launchers) {
      LOG.info("Got container {}", processLauncher.getContainerInfo().getId());
      ProvisionRequest provisionRequest = provisioning.peek();
      if (provisionRequest == null) {
        continue;
      }

      String runnableName = provisionRequest.getRuntimeSpec().getName();
      LOG.info("Starting runnable {} with {}", runnableName, processLauncher);

      LOG.debug("Log level for Twill runnable {} is {}", runnableName, System.getenv(EnvKeys.TWILL_APP_LOG_LEVEL));

      int containerCount = expectedContainers.getExpected(runnableName);

      ProcessLauncher.PrepareLaunchContext launchContext = processLauncher.prepareLaunch(
        ImmutableMap.<String, String>builder()
          .put(EnvKeys.TWILL_APP_DIR, System.getenv(EnvKeys.TWILL_APP_DIR))
          .put(EnvKeys.TWILL_FS_USER, System.getenv(EnvKeys.TWILL_FS_USER))
          .put(EnvKeys.TWILL_APP_RUN_ID, runId.getId())
          .put(EnvKeys.TWILL_APP_NAME, twillSpec.getName())
          .put(EnvKeys.TWILL_APP_LOG_LEVEL, System.getenv(EnvKeys.TWILL_APP_LOG_LEVEL))
          .put(EnvKeys.TWILL_ZK_CONNECT, zkClient.getConnectString())
          .put(EnvKeys.TWILL_LOG_KAFKA_ZK, getKafkaZKConnect())
          .build()
        , getLocalizeFiles(), credentials
      );

      TwillContainerLauncher launcher = new TwillContainerLauncher(
        twillSpec.getRunnables().get(runnableName), launchContext,
        ZKClients.namespace(zkClient, getZKNamespace(runnableName)),
        containerCount, jvmOpts, reservedMemory, getSecureStoreLocation());

      runningContainers.start(runnableName, processLauncher.getContainerInfo(), launcher);

      // Need to call complete to workaround bug in YARN AMRMClient
      if (provisionRequest.containerAcquired()) {
        amClient.completeContainerRequest(provisionRequest.getRequestId());
      }

      if (expectedContainers.getExpected(runnableName) == runningContainers.count(runnableName) ||
        provisioning.peek().getType().equals(AllocationSpecification.Type.ALLOCATE_ONE_INSTANCE_AT_A_TIME)) {
        provisioning.poll();
      }
      if (expectedContainers.getExpected(runnableName) == runningContainers.count(runnableName)) {
        LOG.info("Runnable " + runnableName + " fully provisioned with " + containerCount + " instances.");
      }
    }
  }

  private List<LocalFile> getLocalizeFiles() {
    try (Reader reader = Files.newReader(new File(Constants.Files.LOCALIZE_FILES), Charsets.UTF_8)) {
      return new GsonBuilder().registerTypeAdapter(LocalFile.class, new LocalFileCodec())
        .create().fromJson(reader, new TypeToken<List<LocalFile>>() {
        }.getType());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private String getZKNamespace(String runnableName) {
    return String.format("/%s/runnables/%s", runId.getId(), runnableName);
  }

  private String getKafkaZKConnect() {
    return String.format("%s/%s/kafka", zkClient.getConnectString(), runId.getId());
  }

  /**
   * Attempts to change the number of running instances.
   * @return {@code true} if the message does requests for changes in number of running instances of a runnable,
   *         {@code false} otherwise.
   */
  private boolean handleSetInstances(final Message message, final Runnable completion) {
    if (message.getType() != Message.Type.SYSTEM || message.getScope() != Message.Scope.RUNNABLE) {
      return false;
    }

    Command command = message.getCommand();
    Map<String, String> options = command.getOptions();
    if (!"instances".equals(command.getCommand()) || !options.containsKey("count")) {
      return false;
    }

    final String runnableName = message.getRunnableName();
    if (runnableName == null || runnableName.isEmpty() || !twillSpec.getRunnables().containsKey(runnableName)) {
      LOG.info("Unknown runnable {}", runnableName);
      return false;
    }

    final int newCount = Integer.parseInt(options.get("count"));
    final int oldCount = expectedContainers.getExpected(runnableName);

    LOG.info("Received change instances request for {}, from {} to {}.", runnableName, oldCount, newCount);

    if (newCount == oldCount) {   // Nothing to do, simply complete the request.
      completion.run();
      return true;
    }

    instanceChangeExecutor.execute(createSetInstanceRunnable(message, completion, oldCount, newCount));
    return true;
  }

  /**
   * Creates a Runnable for execution of change instance request.
   */
  private Runnable createSetInstanceRunnable(final Message message, final Runnable completion,
                                             final int oldCount, final int newCount) {
    return new Runnable() {
      @Override
      public void run() {
        final String runnableName = message.getRunnableName();

        LOG.info("Processing change instance request for {}, from {} to {}.", runnableName, oldCount, newCount);
        try {
          // Wait until running container count is the same as old count
          runningContainers.waitForCount(runnableName, oldCount);
          LOG.info("Confirmed {} containers running for {}.", oldCount, runnableName);

          expectedContainers.setExpected(runnableName, newCount);

          try {
            if (newCount < oldCount) {
              // Shutdown some running containers
              for (int i = 0; i < oldCount - newCount; i++) {
                runningContainers.stopLastAndWait(runnableName);
              }
            } else {
              // Increase the number of instances
              runnableContainerRequests.add(createRunnableContainerRequest(runnableName, newCount - oldCount));
            }
          } finally {
            // Send a message to all running runnables that number of instances have changed
            runningContainers.sendToRunnable(runnableName, message, completion);
            LOG.info("Change instances request completed. From {} to {}.", oldCount, newCount);
          }
        } catch (InterruptedException e) {
          // If the wait is being interrupted, discard the message.
          completion.run();
        }
      }
    };
  }

  private RunnableContainerRequest createRunnableContainerRequest(final String runnableName) {
    return createRunnableContainerRequest(runnableName, 1);
  }

  private RunnableContainerRequest createRunnableContainerRequest(final String runnableName,
                                                                  final int numberOfInstances) {
    // Find the current order of the given runnable in order to create a RunnableContainerRequest.
    TwillSpecification.Order order = Iterables.find(twillSpec.getOrders(), new Predicate<TwillSpecification.Order>() {
      @Override
      public boolean apply(TwillSpecification.Order input) {
        return (input.getNames().contains(runnableName));
      }
    });

    RuntimeSpecification runtimeSpec = twillSpec.getRunnables().get(runnableName);
    Resource capability = createCapability(runtimeSpec.getResourceSpecification());
    Map<AllocationSpecification, Collection<RuntimeSpecification>> requestsMap = Maps.newHashMap();
    if (placementPolicyManager.getPlacementPolicyType(runnableName).equals(
      TwillSpecification.PlacementPolicy.Type.DISTRIBUTED)) {
      for (int instanceId = 0; instanceId < numberOfInstances; instanceId++) {
        AllocationSpecification allocationSpecification =
          new AllocationSpecification(capability, AllocationSpecification.Type.ALLOCATE_ONE_INSTANCE_AT_A_TIME,
                                      runnableName, instanceId);
        addAllocationSpecification(allocationSpecification, requestsMap, runtimeSpec);
      }
    } else {
      AllocationSpecification allocationSpecification = new AllocationSpecification(capability);
      addAllocationSpecification(allocationSpecification, requestsMap, runtimeSpec);
    }
    return new RunnableContainerRequest(order.getType(), requestsMap);
  }

  private Runnable getMessageCompletion(final String messageId, final SettableFuture<String> future) {
    return new Runnable() {
      @Override
      public void run() {
        future.set(messageId);
      }
    };
  }

  private Resource createCapability(ResourceSpecification resourceSpec) {
    Resource capability = Records.newRecord(Resource.class);

    if (!YarnUtils.setVirtualCores(capability, resourceSpec.getVirtualCores())) {
      LOG.debug("Virtual cores limit not supported.");
    }

    capability.setMemory(resourceSpec.getMemorySize());
    return capability;
  }

  /**
   * Attempt to restart some instances from a runnable or some runnables.
   * @return {@code true} if the message requests restarting some instances and {@code false} otherwise.
   */
  private boolean handleRestartRunnablesInstances(final Message message, final Runnable completion) {
    LOG.debug("Check if it should process a restart runnable instances.");

    if (message.getType() != Message.Type.SYSTEM) {
      return false;
    }

    Message.Scope messageScope = message.getScope();
    if (messageScope != Message.Scope.RUNNABLE && messageScope != Message.Scope.RUNNABLES) {
      return false;
    }

    Command requestCommand = message.getCommand();
    if (!Constants.RESTART_ALL_RUNNABLE_INSTANCES.equals(requestCommand.getCommand()) &&
      !Constants.RESTART_RUNNABLES_INSTANCES.equals(requestCommand.getCommand())) {
      return false;
    }

    LOG.debug("Processing restart runnable instances message {}.", message);

    if (!Strings.isNullOrEmpty(message.getRunnableName()) && message.getScope() == Message.Scope.RUNNABLE) {
      // ... for a runnable ...
      String runnableName = message.getRunnableName();
      LOG.debug("Start restarting all runnable {} instances.", runnableName);
      restartRunnableInstances(runnableName, null);
    } else {
      // ... or maybe some runnables
      for (Map.Entry<String, String> option : requestCommand.getOptions().entrySet()) {
        String runnableName = option.getKey();
        Set<Integer> restartedInstanceIds = GSON.fromJson(option.getValue(),
                                                           new TypeToken<Set<Integer>>() { }.getType());

        LOG.debug("Start restarting runnable {} instances {}", runnableName, restartedInstanceIds);
        restartRunnableInstances(runnableName, restartedInstanceIds);
      }
    }

    completion.run();
    return true;
  }

  /**
   * Helper method to restart instances of runnables.
   */
  private void restartRunnableInstances(String runnableName, @Nullable Set<Integer> instanceIds) {
    LOG.debug("Begin restart runnable {} instances.", runnableName);

    int runningCount = runningContainers.count(runnableName);
    Set<Integer> instancesToRemove = instanceIds == null ? null : ImmutableSet.copyOf(instanceIds);
    if (instancesToRemove == null) {
      instancesToRemove = Ranges.closedOpen(0, runningCount).asSet(DiscreteDomains.integers());
    }

    for (int instanceId : instancesToRemove) {
      LOG.debug("Stop instance {} for runnable {}", instanceId, runnableName);
      try {
        restartingRunnables.add(runnableName + "-" + instanceId);
        runningContainers.stopByIdAndWait(runnableName, instanceId);
      } catch (Exception ex) {
        // could be thrown if the container already stopped.
        LOG.info("Exception thrown when stopping instance {} probably already stopped.", instanceId);
      }
    }
    LOG.info("Restarting instances {} for runnable {}", instancesToRemove, runnableName);
    runnableContainerRequests.add(createRunnableContainerRequest(runnableName, instancesToRemove.size()));
    // For all runnables that needs to re-request for containers, update the expected count timestamp
    // so that the EventHandler would be triggered with the right expiration timestamp.
    expectedContainers.updateRequestTime(Collections.singleton(runnableName));
    restartingRunnables.clear();
  }
}
