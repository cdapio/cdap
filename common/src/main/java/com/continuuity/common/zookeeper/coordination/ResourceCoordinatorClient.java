/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.zookeeper.ZKExtOperations;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.TreeBasedTable;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nullable;

/**
 * This class helps client to participate in resource coordination process.
 */
public final class ResourceCoordinatorClient extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceCoordinatorClient.class);
  private static final Function<NodeData, ResourceRequirement> NODE_DATA_TO_REQUIREMENT =
    new Function<NodeData, ResourceRequirement>() {
      @Override
      public ResourceRequirement apply(@Nullable NodeData input) {
        if (input == null) {
          return null;
        }
        String json = Bytes.toString(input.getData());
        return CoordinationConstants.GSON.fromJson(json, ResourceRequirement.class);
      }
  };

  private final ZKClient zkClient;
  private final Table<String, Discoverable, ResourceHandlerCaller> resourceHandlers;
  private final Set<String> serviceWatched;
  private final Map<String, ResourceAssignment> assignments;
  private ExecutorService handlerExecutor;

  public ResourceCoordinatorClient(ZKClient zkClient) {
    this.zkClient = zkClient;
    this.resourceHandlers = TreeBasedTable.create(Ordering.natural(), DiscoverableComparator.COMPARATOR);
    this.serviceWatched = Sets.newHashSet();
    this.assignments = Maps.newHashMap();
  }

  /**
   * Submits the given {@link ResourceRequirement} for allocation.
   *
   * @param requirement The requirement to be submitted.
   * @return A {@link ListenableFuture} that will be completed when submission is completed and it'll carry the
   *         submitted requirement as result. The future will fail if failed to submit the requirement. Calling
   *         {@link ListenableFuture#cancel(boolean)} has no effect.
   */
  public ListenableFuture<ResourceRequirement> submitRequirement(final ResourceRequirement requirement) {
    String zkPath = CoordinationConstants.REQUIREMENTS_PATH + "/" + requirement.getName();
    byte[] data = Bytes.toBytes(CoordinationConstants.GSON.toJson(requirement));

    return ZKExtOperations.createOrSet(zkClient, zkPath, data, requirement, CoordinationConstants.MAX_ZK_FAILURE_RETRY);
  }

  /**
   * Fetches the {@link ResourceRequirement} for the given resource.
   *
   * @param resourceName Name of the resource.
   * @return A {@link ListenableFuture} that will be completed when the requirement is fetch. A {@code null} result
   *         will be set into the future if no such requirement exists. The future will fail if failed to fetch
   *         the requirement due to error other than requirement not exists.
   *         Calling {@link ListenableFuture#cancel(boolean)} has no effect.
   */
  public ListenableFuture<ResourceRequirement> fetchRequirement(String resourceName) {
    String zkPath = CoordinationConstants.REQUIREMENTS_PATH + "/" + resourceName;

    return Futures.transform(
      ZKOperations.ignoreError(zkClient.getData(zkPath), KeeperException.NoNodeException.class, null),
      NODE_DATA_TO_REQUIREMENT
    );
  }

  /**
   * Deletes the {@link ResourceRequirement} for the given resource.
   *
   * @param resourceName Name of the resource.
   * @return A {@link ListenableFuture} that will be completed when the requirement is successfully removed.
   *         If the requirement doesn't exists, the deletion would still be treated as successful.
   */
  public ListenableFuture<String> deleteRequirement(String resourceName) {
    String zkPath = CoordinationConstants.REQUIREMENTS_PATH + "/" + resourceName;

    return Futures.transform(
      ZKOperations.ignoreError(zkClient.delete(zkPath), KeeperException.NoNodeException.class, resourceName),
      Functions.constant(resourceName)
    );
  }

  /**
   * Subscribes for changes in resource assignment for the given {@link Discoverable}. Upon subscription started,
   * the {@link ResourceHandler#onChange(java.util.Collection)} method will get called to receive the current
   * assignment for the Discoverable if it exists.
   *
   * @param discoverable The discoverable that is interested for changes in resource assignment.
   * @param handler The {@link ResourceHandler} that get notified why resource assignment changed.
   * @return A {@link Cancellable} for cancelling the watch.
   */
  public synchronized Cancellable subscribe(final Discoverable discoverable, final ResourceHandler handler) {
    final String serviceName = discoverable.getName();
    final ResourceHandlerCaller caller = new ResourceHandlerCaller(discoverable, handler);

    if (serviceWatched.add(serviceName)) {
      // Not yet watching ZK, add the handler and start watching ZK for changes in assignment.
      resourceHandlers.put(serviceName, discoverable, caller);
      watchAssignment(serviceName);
    } else {
      // Invoke the handler for the cached assignment if there is any before adding to the resource handler list
      ResourceAssignment assignment = assignments.get(serviceName);
      if (assignment != null) {
        Collection<PartitionReplica> partitionReplicas = assignment.getAssignments().get(discoverable);
        if (!partitionReplicas.isEmpty()) {
          caller.onChange(partitionReplicas);
        }
      }
      resourceHandlers.put(serviceName, discoverable, caller);
    }

    return new ResourceHandlerCancellable(caller);
  }

  @Override
  protected void doStart() {
    handlerExecutor = Executors.newSingleThreadExecutor(
      Threads.createDaemonThreadFactory("resource-coordinator-client"));
    notifyStarted();
  }

  @Override
  protected void doStop() {
    try {
      finishHandlers(null);
      notifyStopped();
    } finally {
      handlerExecutor.shutdown();
    }
  }

  private void doNotifyFailed(Throwable cause) {
    try {
      finishHandlers(cause);
    } finally {
      handlerExecutor.shutdown();
      notifyFailed(cause);
    }
  }

  /**
   * Calls the {@link ResourceHandler#finished(Throwable)} method on all existing handlers.
   *
   * @param failureCause Failure reason for finish or {@code null} if finish is not due to failure.
   */
  private synchronized void finishHandlers(Throwable failureCause) {
    for (ResourceHandlerCaller caller : resourceHandlers.values()) {
      caller.finished(failureCause);
    }
  }

  /**
   * Starts watching ZK for ResourceAssignment changes for the given service.
   */
  private void watchAssignment(final String serviceName) {
    final String zkPath = CoordinationConstants.ASSIGNMENTS_PATH + "/" + serviceName;

    // Watch for both getData() and exists() call
    Watcher watcher = wrapWatcher(new AssignmentWatcher(serviceName, EnumSet.of(Watcher.Event.EventType.NodeDataChanged,
                                                                                Watcher.Event.EventType.NodeDeleted)));

    Futures.addCallback(zkClient.getData(zkPath, watcher), wrapCallback(new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        String json = Bytes.toString(result.getData());
        LOG.debug("Received resource assignment for {}. {}", serviceName, json);
        try {
          ResourceAssignment assignment = CoordinationConstants.GSON.fromJson(json, ResourceAssignment.class);
          handleAssignmentChange(serviceName, assignment);
        } catch (Exception e) {
          LOG.error("Failed to decode ResourceAssignment {}", json, e);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (t instanceof KeeperException.NoNodeException) {
          // Treat it as assignment has been removed. If the node doesn't exists for the first time fetch data,
          // there will be no oldAssignment, hence the following call would be a no-op.
          handleAssignmentChange(serviceName, new ResourceAssignment(serviceName));

          // Watch for exists if it still interested
          synchronized (ResourceCoordinatorClient.this) {
            if (!resourceHandlers.row(serviceName).isEmpty()) {
              watchAssignmentOnExists(serviceName);
            }
          }
        } else {
          LOG.error("Failed to getData on ZK {}{}", zkClient.getConnectString(), zkPath, t);
          doNotifyFailed(t);
        }
      }
    }), Threads.SAME_THREAD_EXECUTOR);
  }

  /**
   * Starts watch for assignment changes when the node exists.
   *
   * @param serviceName Name of the service.
   */
  private void watchAssignmentOnExists(final String serviceName) {
    final String zkPath = CoordinationConstants.ASSIGNMENTS_PATH + "/" + serviceName;
    Watcher watcher = wrapWatcher(new AssignmentWatcher(serviceName, EnumSet.of(Watcher.Event.EventType.NodeCreated)));
    Futures.addCallback(zkClient.exists(zkPath, watcher), wrapCallback(new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result != null) {
          watchAssignment(serviceName);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to call exists on ZK {}{}", zkClient.getConnectString(), zkPath, t);
        doNotifyFailed(t);
      }
    }), Threads.SAME_THREAD_EXECUTOR);
  }

  /**
   * Handles changes in assignment.
   *
   * @param newAssignment The updated assignment.
   */
  private synchronized void handleAssignmentChange(String serviceName, ResourceAssignment newAssignment) {
    ResourceAssignment oldAssignment = assignments.get(serviceName);

    // Nothing changed.
    if (Objects.equal(oldAssignment, newAssignment)) {
      return;
    }

    Collection<PartitionReplica> emptyAssignment = ImmutableList.of();
    // If the new assignment is empty, simply remove it from cache, otherwise remember it.
    if (newAssignment.getAssignments().isEmpty()) {
      assignments.remove(serviceName);
    } else {
      assignments.put(serviceName, newAssignment);
    }

    // For each new assignment, see if it has been changed by comparing with the old assignment.
    Map<Discoverable, Collection<PartitionReplica>> assignmentMap = newAssignment.getAssignments().asMap();
    for (Map.Entry<Discoverable, Collection<PartitionReplica>> entry : assignmentMap.entrySet()) {
      if (oldAssignment == null || !oldAssignment.getAssignments().get(entry.getKey()).equals(entry.getValue())) {
        // Assignment has been changed, notify the handler if there is one.
        ResourceHandlerCaller caller = resourceHandlers.get(serviceName, entry.getKey());
        if (caller != null) {
          caller.onChange(ImmutableList.copyOf(entry.getValue()));
        }
      }
    }

    // For each old assignment, if it is no long exists in the new one, it should invoke with a empty change.
    if (oldAssignment != null) {
      Sets.SetView<Discoverable> discoverableRemoved = Sets.difference(oldAssignment.getAssignments().keySet(),
                                                                       newAssignment.getAssignments().keySet());
      for (Discoverable discoverable : discoverableRemoved) {
        ResourceHandlerCaller caller = resourceHandlers.get(serviceName, discoverable);
        if (caller != null) {
          caller.onChange(emptyAssignment);
        }
      }
    }
  }

  /**
   * Wraps a ZK watcher so that it only get triggered if this service is running.
   *
   * @param watcher The Watcher to wrap.
   * @return A wrapped Watcher.
   */
  private Watcher wrapWatcher(final Watcher watcher) {
    return new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (isRunning()) {
          watcher.process(event);
        }
      }
    };
  }

  /**
   * Wraps a FutureCallback so that it only get triggered if this service is running.
   *
   * @param callback The callback to wrap.
   * @param <V> Type of the callback result.
   * @return A wrapped FutureCallback.
   */
  private <V> FutureCallback<V> wrapCallback(final FutureCallback<V> callback) {
    return new FutureCallback<V>() {
      @Override
      public void onSuccess(V result) {
        if (isRunning()) {
          callback.onSuccess(result);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (isRunning()) {
          callback.onFailure(t);
        }
      }
    };
  }

  /**
   * Wraps a {@link ResourceHandler} so that it's always invoked from the handler executor. It also make sure
   * change event only get fired to the intended Discoverable.
   */
  private final class ResourceHandlerCaller implements ResourceHandler {

    private final Discoverable targetDiscoverable;
    private final ResourceHandler delegate;

    ResourceHandlerCaller(Discoverable targetDiscoverable, ResourceHandler delegate) {
      this.targetDiscoverable = targetDiscoverable;
      this.delegate = delegate;
    }

    public Discoverable getDiscoverable() {
      return targetDiscoverable;
    }

    @Override
    public void onChange(final Collection<PartitionReplica> partitionReplicas) {
      handlerExecutor.execute(new Runnable() {
        @Override
        public void run() {
          delegate.onChange(partitionReplicas);
        }
      });
    }

    @Override
    public void finished(final Throwable failureCause) {
      // Remove itself from the handlers and only invoke finish call if successfully removing itself.
      synchronized (ResourceCoordinatorClient.this) {
        if (resourceHandlers.remove(targetDiscoverable.getName(), targetDiscoverable) != this) {
          return;
        }
      }

      handlerExecutor.execute(new Runnable() {

        @Override
        public void run() {
          delegate.finished(failureCause);
        }
      });
    }
  }

  /**
   * ZK Watcher to set on the resource assignment node. It's used for both getData and exists call.
   */
  private final class AssignmentWatcher implements Watcher {

    private final String serviceName;
    private final EnumSet<Event.EventType> actOnTypes;

    AssignmentWatcher(String serviceName, EnumSet<Event.EventType> actOnTypes) {
      this.serviceName = serviceName;
      this.actOnTypes = actOnTypes;
    }

    @Override
    public void process(WatchedEvent event) {
      if (actOnTypes.contains(event.getType())) {
        // If no handler is interested in the event, simply ignore the event and not setting the watch again.
        synchronized (ResourceCoordinatorClient.this) {
          if (resourceHandlers.row(serviceName).isEmpty()) {
            serviceWatched.remove(serviceName);
            return;
          }
        }
        // If some handler exists, call watchAssignment again to fetch the data and set the Watch.
        watchAssignment(serviceName);
      }
    }
  }


  /**
   * Cancellable for removing handler from the handler list.
   */
  private final class ResourceHandlerCancellable implements Cancellable {
    private final ResourceHandlerCaller caller;

    public ResourceHandlerCancellable(ResourceHandlerCaller caller) {
      this.caller = caller;
    }

    @Override
    public void cancel() {
      caller.finished(null);
    }
  }
}
