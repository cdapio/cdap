/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.zookeeper.ZKExtOperations;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Allocate resources to registered handler. It is expected to have single instance of this class
 * running per ZK namespace (determined by the ZKClient passed to constructor). User can use
 * {@link com.continuuity.common.zookeeper.election.LeaderElection} class to help archiving requirement.
 *
 * ZK structure.
 *
 * <pre>
 * {@code
 * /requirements
 *     /[resource_name] - Contains ResourceRequirement encoded in json
 * /assignments
 *     /[resource_name] - Contains ResourceAssignment encoded in json
 * }
 * </pre>
 *
 */
public final class ResourceCoordinator extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceCoordinator.class);

  private final ZKClient zkClient;
  private final DiscoveryServiceClient discoveryService;
  private final AssignmentStrategy assignmentStrategy;
  private final Map<String, ResourceRequirement> requirements;
  private final Map<String, ResourceAssignment> assignments;
  private final Map<String, CancellableServiceDiscovered> serviceDiscovered;
  private final DiscoverableChangeListener discoverableListener;

  // A single thread executor to process resource requests and perform allocation.
  private ExecutorService executor;

  public ResourceCoordinator(ZKClient zkClient,
                             DiscoveryServiceClient discoveryService,
                             AssignmentStrategy assignmentStrategy) {
    this.zkClient = zkClient;
    this.discoveryService = discoveryService;
    this.assignmentStrategy = assignmentStrategy;
    this.requirements = Maps.newHashMap();
    this.assignments = Maps.newHashMap();
    this.serviceDiscovered = Maps.newHashMap();
    this.discoverableListener = new DiscoverableChangeListener();
  }


  @Override
  protected void doStart() {
    executor = Executors.newSingleThreadExecutor(Threads.createDaemonThreadFactory("resource-coordinator"));
    beginWatch(wrapWatcher(new ResourceWatcher()));
    notifyStarted();
  }

  @Override
  protected void doStop() {
    try {
      executor.execute(createShutdownTask(null));
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Signals failure in this service and terminates itself.
   *
   * @param cause Reason for failure.
   */
  private void doNotifyFailed(Throwable cause) {
    try {
      executor.execute(createShutdownTask(cause));
    } finally {
      executor.shutdown();
    }
  }

  private Runnable createShutdownTask(final Throwable failureCause) {
    return new Runnable() {
      @Override
      public void run() {
        for (Cancellable cancellable : serviceDiscovered.values()) {
          try {
            cancellable.cancel();
          } catch (Throwable t) {
            LOG.warn("Exception when cancelling service discovery listener.", t);
          }
        }
        if (failureCause == null) {
          notifyStopped();
        } else {
          notifyFailed(failureCause);
        }
      }
    };
  }

  /**
   * Start watching for changes in resources requirements.
   */
  private void beginWatch(final Watcher watcher) {
    Futures.addCallback(zkClient.exists(CoordinationConstants.REQUIREMENTS_PATH, watcher),
                        wrapCallback(new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result != null) {
          fetchAndProcessAllResources(watcher);
        }
        // If the node doesn't exists yet, that's ok, the watcher would handle it once it's created.
      }

      @Override
      public void onFailure(Throwable t) {
        // Something very wrong to have exists call failed.
        LOG.error("Failed to call exists on ZK node {}{}",
                  zkClient.getConnectString(), CoordinationConstants.REQUIREMENTS_PATH, t);
        doNotifyFailed(t);
      }
    }), executor);
  }

  /**
   * Fetches all {@link ResourceRequirement} and perform assignment for the one that changed. Also, it will
   * remove assignments for the resource requirements that are removed.
   */
  private void fetchAndProcessAllResources(final Watcher watcher) {
    Futures.addCallback(
      zkClient.getChildren(CoordinationConstants.REQUIREMENTS_PATH, watcher),
      wrapCallback(new FutureCallback<NodeChildren>() {
        @Override
        public void onSuccess(NodeChildren result) {
          Set<String> children = ImmutableSet.copyOf(result.getChildren());

          // Handle new resources
          for (String child : children) {
            String path = CoordinationConstants.REQUIREMENTS_PATH + "/" + child;
            Watcher requirementWatcher = wrapWatcher(new ResourceRequirementWatcher(path));
            fetchAndProcessRequirement(path, requirementWatcher);
          }

          // Handle removed resources
          for (String removed: ImmutableSet.copyOf(Sets.difference(requirements.keySet(), children))) {
            ResourceRequirement requirement = requirements.remove(removed);
            LOG.info("Requirement deleted {}", requirement);
            // Delete the assignment node.
            removeAssignment(removed);
          }
        }

        @Override
        public void onFailure(Throwable t) {
          // If the resource path node doesn't exists, resort to watch for exists.
          if (t instanceof KeeperException.NoNodeException) {
            beginWatch(watcher);
          }
          // Otherwise, it's a unexpected failure.
          LOG.error("Failed to getChildren on ZK node {}{}",
                    zkClient.getConnectString(), CoordinationConstants.REQUIREMENTS_PATH, t);
          doNotifyFailed(t);
        }
      }), executor);
  }

  /**
   * Gets the data from a resource node, decode it to {@link ResourceRequirement} and performs resource assignment
   * if the requirement changed.
   */
  private void fetchAndProcessRequirement(final String path, Watcher watcher) {
    Futures.addCallback(zkClient.getData(path, watcher), wrapCallback(new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        byte[] nodeData = result.getData();
        if (nodeData == null) {
          LOG.warn("Ignore empty data in ZK node {}{}", zkClient.getConnectString(), path);
          return;
        }

        try {
          ResourceRequirement requirement = CoordinationConstants.RESOURCE_REQUIREMENT_CODEC.decode(nodeData);
          LOG.info("Get requirement {}", requirement);

          // See if the requirement changed.
          ResourceRequirement oldRequirement = requirements.get(requirement.getName());
          if (requirement.equals(oldRequirement)) {
            LOG.info("Requirement for {} is not changed. No assignment is needed. {} = {}",
                     requirement.getName(), oldRequirement, requirement);
            return;
          }

          // Requirement change, perform assignment, optional subscribe to service discovery if not yet did.
          requirements.put(requirement.getName(), requirement);

          CancellableServiceDiscovered discovered = serviceDiscovered.get(requirement.getName());
          if (discovered == null) {
            discovered = new CancellableServiceDiscovered(discoveryService.discover(requirement.getName()),
                                                          discoverableListener, executor);
            serviceDiscovered.put(requirement.getName(), discovered);

            // If it is the first time it subscribes, no need to trigger assignment logic here as the first call
            // to listener.onChange would do so.
          } else {
            performAssignment(requirement, ImmutableSortedSet.copyOf(DiscoverableComparator.COMPARATOR, discovered));
          }

        } catch (Exception e) {
          LOG.warn("Failed to process requirement ZK node {}{}: {}",
                   zkClient.getConnectString(), path, Bytes.toStringBinary(nodeData), e);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // Just log
        LOG.error("Failed to getData on ZK node {}{}", zkClient.getConnectString(), path, t);
      }
    }), executor);
  }

  /**
   * Performs resource assignment based on the resource requirement.
   * This method should only be called from the single thread executor owned by this class.
   * 
   * @param requirement The resource requirement that needs to be fulfilled.
   * @param handlers The set of handlers available.
   */
  private void performAssignment(ResourceRequirement requirement, Set<Discoverable> handlers) {
    ResourceAssignment oldAssignment = assignments.get(requirement.getName());

    if (oldAssignment == null) {
      // Try to fetch it from ZK.
      // This is the penalty to pay for the first time this coordinator instance sees a given requirement.
      fetchAndPerformAssignment(requirement, handlers);
      return;
    }

    // Build a map from partition name to number of replicas
    Map<String, Integer> partitions = Maps.newHashMap();
    for (ResourceRequirement.Partition partition : requirement.getPartitions()) {
      partitions.put(partition.getName(), partition.getReplicas());
    }

    // Copy all valid partition replicas assignments and drops the invalid one.
    Multimap<Discoverable, PartitionReplica> assignmentMap =
      TreeMultimap.create(DiscoverableComparator.COMPARATOR, PartitionReplica.COMPARATOR);

    for (Map.Entry<Discoverable, PartitionReplica> entry : oldAssignment.getAssignments().entries()) {
      Integer replicas = partitions.get(entry.getValue().getName());
      // Check if the partition replica is still valid and if the handler being assigned still exists.
      if (replicas != null && entry.getValue().getReplicaId() < replicas && handlers.contains(entry.getKey())) {
        assignmentMap.put(entry.getKey(), entry.getValue());
      }
    }

    ResourceAssigner<Discoverable> assigner = DefaultResourceAssigner.create(assignmentMap);

    // Call the strategy for assignment only if there are some handlers.
    if (!handlers.isEmpty()) {
      assignmentStrategy.assign(requirement, handlers, assigner);
    }

    // Save the new assignment
    saveAssignment(new ResourceAssignment(requirement.getName(), assigner.get()));
  }

  /**
   * Fetch the {@link ResourceAssignment} from ZK and then perform resource assignment logic. This is done with best
   * effort to let the {@link AssignmentStrategy} has access to existing assignment. If failed to get existing
   * {@link ResourceAssignment} or if it's simply not exists, assignment will still be triggered as if there is no
   * existing assignment.
   *
   * @param requirement The resource requirement that needs to be fulfilled.
   * @param handlers The set of handlers available.
   */
  private void fetchAndPerformAssignment(final ResourceRequirement requirement, final Set<Discoverable> handlers) {
    final String name = requirement.getName();
    String zkPath = CoordinationConstants.ASSIGNMENTS_PATH + "/" + name;
    Futures.addCallback(zkClient.getData(zkPath), new FutureCallback<NodeData>() {

      @Override
      public void onSuccess(NodeData result) {
        if (assignments.get(name) != null) {
          // Assignment should has been performed while this one is fetching. So, ignore this.
          return;
        }
        byte[] data = result.getData();
        ResourceAssignment resourceAssignment = new ResourceAssignment(name);
        try {
          if (data != null) {
            resourceAssignment = CoordinationConstants.RESOURCE_ASSIGNMENT_CODEC.decode(data);
          }
        } catch (Throwable t) {
          LOG.warn("Failed to decode resource assignment. Perform assignment as if no assignment existed.", t);
        }

        assignments.put(name, resourceAssignment);
        performAssignment(requirement, handlers);
      }

      @Override
      public void onFailure(Throwable t) {
        if (!(t instanceof KeeperException.NoNodeException)) {
          // If failure is not because node doesn't exists, log a warning
          LOG.warn("Failed to fetch current assignment. Perform assignment as if no assignment existed.", t);
        }
        assignments.put(name, new ResourceAssignment(name));
        performAssignment(requirement, handlers);
      }
    }, executor);
  }

  /**
   * Save a {@link ResourceAssignment} to local cache as well as ZK ZK.
   * @param assignment The assignment to be persisted.
   */
  private void saveAssignment(ResourceAssignment assignment) {
    assignments.put(assignment.getName(), assignment);

    try {
      final byte[] data = CoordinationConstants.RESOURCE_ASSIGNMENT_CODEC.encode(assignment);
      String zkPath = CoordinationConstants.ASSIGNMENTS_PATH + "/" + assignment.getName();

      Futures.addCallback(
        ZKExtOperations.setOrCreate(zkClient, zkPath, data, assignment, CoordinationConstants.MAX_ZK_FAILURE_RETRY),
        new FutureCallback<ResourceAssignment>() {

          @Override
          public void onSuccess(ResourceAssignment result) {
            // Done. Just log for debugging.
            LOG.debug("Resource assignment updated for {}. {}", result.getName(), Bytes.toString(data));
          }

          @Override
          public void onFailure(Throwable t) {
            LOG.error("Failed to save assignment {}", Bytes.toStringBinary(data), t);
            doNotifyFailed(t);
          }
        }, executor
      );
    } catch (Exception e) {
      // Something very wrong
      LOG.error("Failed to save assignment: {}", assignment.getName(), e);
    }
  }

  /**
   * Removes the {@link ResourceAssignment} with the given name from local cache as well as ZK.
   * @param name Name of the resource.
   */
  private void removeAssignment(String name) {
    assignments.remove(name);

    String zkPath = CoordinationConstants.ASSIGNMENTS_PATH + "/" + name;

    // Simply delete the assignment node. No need to care about the result.
    // Even if failed to delete the node and leaves stale assignment, next time when an assignment action is
    // triggered, it'll correct it.
    zkClient.delete(zkPath);
  }

  /**
   * Returns true if this service is up and running, hence should process any events that it received.
   */
  private boolean shouldProcess() {
    State state = state();
    return state == State.STARTING || state == State.RUNNING;
  }

  /**
   * Wraps a given callback so that it only get triggered if {@link #shouldProcess()} returns true.
   */
  private <T> FutureCallback<T> wrapCallback(final FutureCallback<T> callback) {
    return new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        if (shouldProcess()) {
          callback.onSuccess(result);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (shouldProcess()) {
          callback.onFailure(t);
        }
      }
    };
  }

  /**
   * Wraps a given Watcher so that it only get triggered if {@link #shouldProcess()} returns true.
   */
  private Watcher wrapWatcher(final Watcher watcher) {
    return new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!shouldProcess()) {
          return;
        }
        watcher.process(event);
      }
    };
  }

  /**
   * Watcher to handle children nodes changes in the resource requirement. Child node will be added / removed
   * when requirement is added or removed.
   */
  private final class ResourceWatcher implements Watcher {

    @Override
    public void process(WatchedEvent event) {
      if (!shouldProcess()) {
        return;
      }

      switch (event.getType()) {
        case NodeCreated:
        case NodeChildrenChanged:
          fetchAndProcessAllResources(this);
          break;
        case NodeDeleted:
          beginWatch(this);
          break;
        default:
          // No-op
      }
    }
  }

  /**
   * Watcher to get updates in resource requirement node.
   */
  private final class ResourceRequirementWatcher implements Watcher {

    private final String path;

    private ResourceRequirementWatcher(String path) {
      this.path = path;
    }

    @Override
    public void process(WatchedEvent event) {
      if (!shouldProcess()) {
        return;
      }

      // Only interested in data change event. Other type of events is handled by the watcher on parent node.
      if (event.getType() == Event.EventType.NodeDataChanged) {
        fetchAndProcessRequirement(path, this);
      }
    }
  }

  private final class DiscoverableChangeListener implements ServiceDiscovered.ChangeListener {

    @Override
    public void onChange(ServiceDiscovered serviceDiscovered) {
      ResourceRequirement requirement = requirements.get(serviceDiscovered.getName());
      if (requirement != null) {
        performAssignment(requirement, ImmutableSortedSet.copyOf(DiscoverableComparator.COMPARATOR, serviceDiscovered));
      }
    }
  }

  private static final class CancellableServiceDiscovered implements Iterable<Discoverable>, Cancellable {

    private final Cancellable cancellable;
    private final ServiceDiscovered serviceDiscovered;

    private CancellableServiceDiscovered(ServiceDiscovered serviceDiscovered,
                                         ServiceDiscovered.ChangeListener listener, Executor executor) {
      this.cancellable = serviceDiscovered.watchChanges(listener, executor);
      this.serviceDiscovered = serviceDiscovered;
    }

    @Override
    public void cancel() {
      cancellable.cancel();
    }

    @Override
    public Iterator<Discoverable> iterator() {
      return serviceDiscovered.iterator();
    }
  }
}
