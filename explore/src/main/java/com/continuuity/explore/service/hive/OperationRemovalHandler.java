package com.continuuity.explore.service.hive;

import com.continuuity.explore.service.ExploreService;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.Status;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static com.continuuity.explore.service.hive.BaseHiveExploreService.OperationInfo;

/**
 * Takes care of closing operations after they are removed from the cache. It does not close operations that were
 * explicitly removed by the user.
 */
public class OperationRemovalHandler implements Runnable, RemovalListener<Handle, OperationInfo> {
  private static final Logger LOG = LoggerFactory.getLogger(OperationRemovalHandler.class);

  private final BlockingQueue<Handle> operations = new LinkedBlockingQueue<Handle>();
  private final Map<Handle, OperationInfo> deletedHandleMap = Maps.newConcurrentMap();
  private final ExploreService exploreService;

  public OperationRemovalHandler(ExploreService exploreService) {
    this.exploreService = exploreService;
  }

  OperationInfo getDeletedHandle(Handle handle) {
    return deletedHandleMap.get(handle);
  }

  @Override
  public void onRemoval(RemovalNotification<Handle, OperationInfo> notification) {
    LOG.trace("Got removal notification for handle {} with cause {}", notification.getKey(), notification.getCause());
    // Don't add handles explicitly removed by the user.
    if (notification.getCause() != RemovalCause.EXPLICIT) {
      onRemoval(notification.getKey(), notification.getValue());
    }
  }

  void onRemoval(Handle handle, OperationInfo operationInfo) {
    deletedHandleMap.put(handle, operationInfo);
    operations.add(handle);
  }

  @Override
  public void run() {
    Set<Handle> toTimeout = Sets.newHashSet();
    operations.drainTo(toTimeout);

    for (Handle handle : toTimeout) {
      try {
        Status status = exploreService.getStatus(handle);

        // If operation is still not complete, cancel it.
        if (status.getStatus() != Status.OpStatus.FINISHED && status.getStatus() != Status.OpStatus.CLOSED &&
          status.getStatus() != Status.OpStatus.CANCELED && status.getStatus() != Status.OpStatus.ERROR) {
          LOG.info("Cancelling handle {} with status {} due to timeout",
                   handle.getHandle(), status.getStatus());
          exploreService.cancel(handle);
        }

      } catch (Throwable e) {
        LOG.error("Could not cancel handle {} due to exception", handle.getHandle(), e);
      } finally {
        LOG.info("Timing out handle {}", handle);
        try {
          // Finally close the operation
          exploreService.close(handle);
        } catch (Throwable e) {
          LOG.error("Exception while closing handle {}", handle, e);
        } finally {
          deletedHandleMap.remove(handle);
        }
      }
    }
  }
}
