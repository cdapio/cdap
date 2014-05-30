/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.coordination;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.zookeeper.ZKExtOperations;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        try {
          return CoordinationConstants.RESOURCE_REQUIREMENT_CODEC.decode(input.getData());
        } catch (Throwable t) {
          LOG.error("Failed to decode resource requirement: {}", Bytes.toStringBinary(input.getData()), t);
          throw Throwables.propagate(t);
        }
      }
  };

  private final ZKClient zkClient;
  private final Multimap<String, AssignmentChangeListener> changeListeners;
  private final Set<String> serviceWatched;
  private final Map<String, ResourceAssignment> assignments;
  private ExecutorService handlerExecutor;

  public ResourceCoordinatorClient(ZKClient zkClient) {
    this.zkClient = zkClient;
    this.changeListeners = LinkedHashMultimap.create();
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
  public ListenableFuture<ResourceRequirement> submitRequirement(ResourceRequirement requirement) {
    try {
      String zkPath = CoordinationConstants.REQUIREMENTS_PATH + "/" + requirement.getName();
      byte[] data = CoordinationConstants.RESOURCE_REQUIREMENT_CODEC.encode(requirement);

      return ZKExtOperations.createOrSet(zkClient, zkPath, data,
                                         requirement, CoordinationConstants.MAX_ZK_FAILURE_RETRY);
    } catch (Exception e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  /**
   * Modify an existing {@link ResourceRequirement}.
   *
   * @param name Resource name
   * @param modifier A function to modify an existing requirement. The function might get called multiple times
   *                 if there are concurrent modifications from multiple clients.
   * @return A {@link ListenableFuture} that will be completed when submission is completed and it'll carry the
   *         modified requirement as result or {@code null} if the modifier decided not to modify the requirement.
   *         The future will fail if failed to submit the requirement.
   *         Calling {@link ListenableFuture#cancel(boolean)} has no effect.
   */
  public ListenableFuture<ResourceRequirement> modifyRequirement(String name, final ResourceModifier modifier) {
    String zkPath = CoordinationConstants.REQUIREMENTS_PATH + "/" + name;
    return ZKExtOperations.updateOrCreate(zkClient, zkPath, modifier, CoordinationConstants.RESOURCE_REQUIREMENT_CODEC);
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
   * Subscribes for changes in resource assignment. Upon subscription started,
   * the {@link AssignmentChangeListener#onChange(ResourceAssignment)} method will be invoked to receive the
   * current assignment if it exists.
   *
   * @param serviceName Name of the service to watch for changes.
   * @param listener The listener to invoke when there are changes.
   * @return A {@link Cancellable} for cancelling the subscription.
   */
  public synchronized Cancellable subscribe(String serviceName, AssignmentChangeListener listener) {
    AssignmentChangeListenerCaller caller = new AssignmentChangeListenerCaller(serviceName, listener);

    if (serviceWatched.add(serviceName)) {
      // Not yet watching ZK, add the handler and start watching ZK for changes in assignment.
      changeListeners.put(serviceName, caller);
      watchAssignment(serviceName);
    } else {
      // Invoke the listener with the cached assignment if there is any before adding to the resource handler list
      ResourceAssignment assignment = assignments.get(serviceName);
      if (assignment != null && !assignment.getAssignments().isEmpty()) {
        caller.onChange(assignment);
      }
      changeListeners.put(serviceName, caller);
    }

    return new AssignmentListenerCancellable(caller);
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
    for (AssignmentChangeListener listener : changeListeners.values()) {
      listener.finished(failureCause);
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
        try {
          ResourceAssignment assignment = CoordinationConstants.RESOURCE_ASSIGNMENT_CODEC.decode(result.getData());
          LOG.debug("Received resource assignment for {}. {}", serviceName, assignment.getAssignments());

          handleAssignmentChange(serviceName, assignment);
        } catch (Exception e) {
          LOG.error("Failed to decode ResourceAssignment {}", Bytes.toStringBinary(result.getData()), e);
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
            if (changeListeners.containsKey(serviceName)) {
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

    // If the new assignment is empty, simply remove it from cache, otherwise remember it.
    if (newAssignment.getAssignments().isEmpty()) {
      assignments.remove(serviceName);
    } else {
      assignments.put(serviceName, newAssignment);
    }

    // If the change is from null to empty, no need to notify listeners.
    if (oldAssignment == null && newAssignment.getAssignments().isEmpty()) {
      return;
    }

    // Otherwise, notify all listeners
    for (AssignmentChangeListener listener : changeListeners.get(serviceName)) {
      listener.onChange(newAssignment);
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
   * Wraps a {@link AssignmentChangeListener} so that it's always invoked from the handler executor. It also make sure
   * upon {@link AssignmentChangeListener#finished(Throwable)} is called, it get removed from the listener list.
   */
  private final class AssignmentChangeListenerCaller implements AssignmentChangeListener {

    private final String service;
    private final AssignmentChangeListener delegate;

    private AssignmentChangeListenerCaller(String service, AssignmentChangeListener delegate) {
      this.service = service;
      this.delegate = delegate;
    }

    @Override
    public void onChange(final ResourceAssignment assignment) {
      handlerExecutor.execute(new Runnable() {
        @Override
        public void run() {
          delegate.onChange(assignment);
        }
      });
    }

    @Override
    public void finished(final Throwable failureCause) {
      // Remove itself from the handlers and only invoke finish call if successfully removing itself.
      synchronized (ResourceCoordinatorClient.this) {
        if (!changeListeners.remove(service, this)) {
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
          if (!changeListeners.containsKey(serviceName)) {
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
   * Cancellable that delegates to the {@link AssignmentChangeListenerCaller#finished(Throwable)} method.
   */
  private static final class AssignmentListenerCancellable implements Cancellable {
    private final AssignmentChangeListenerCaller caller;

    private AssignmentListenerCancellable(AssignmentChangeListenerCaller caller) {
      this.caller = caller;
    }

    @Override
    public void cancel() {
      caller.finished(null);
    }
  }
}
