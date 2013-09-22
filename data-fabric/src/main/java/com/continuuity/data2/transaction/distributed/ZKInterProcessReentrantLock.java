package com.continuuity.data2.transaction.distributed;

import com.continuuity.weave.zookeeper.NodeChildren;
import com.continuuity.weave.zookeeper.OperationFuture;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

/**
 * A re-entrant mutual exclusion lock backed up by Zookeeper.
 * <b/>
 * Note: this class is not thread-safe. When using within same process every thread should have it's own instance of
 * the lock.
 */
// TODO: remove it in favor of generic leader election tool
public class ZKInterProcessReentrantLock {
  private final ZKClient zkClient;
  private final String path;
  private final String lockPath;
  // node that holds the lock, null if lock is not hold by us
  private String lockNode;

  // todo: consider implementing Locks class to create different types of locks
  public ZKInterProcessReentrantLock(ZKClient zkClient, String path) {
    this.zkClient = zkClient;
    this.path = path;
    this.lockPath = path + "/lock";
    ZKClientExt.ensureExists(zkClient, path);
  }

  public ListenableFuture<Boolean> acquire() {
    if (isOwnerOfLock()) {
      return Futures.immediateFuture(true);
    }

    // The algo is the following:
    // 1) we add sequential ephemeral node
    // 2a) if added node is the first one in the list, we acquired the lock. Finish
    // 2b) if added node is not the first one, then add watch to the one before it to re-acquire when it is deleted.

    lockNode = Futures.getUnchecked(zkClient.create(lockPath, null, CreateMode.EPHEMERAL_SEQUENTIAL, true));
    NodeChildren nodeChildren = Futures.getUnchecked(zkClient.getChildren(path));
    List<String> children = nodeChildren.getChildren();
    Collections.sort(children);
    if (lockNode.equals(path + "/" + children.get(0))) {
      // we are the first to acquire the lock
      return Futures.immediateFuture(true);
    }

    final SettableFuture<Boolean> future = SettableFuture.create();
    boolean setWatcher = false;
    // add watch to the previous node
    Collections.reverse(children);
    for (String child : children) {
      child = path + "/" + child;
      if (child.compareTo(lockNode) < 0) {
        OperationFuture<Stat> exists = zkClient.exists(child, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDeleted) {
              future.set(true);
            }
          }
        });
        // if it was deleted before we managed to add watcher, we need to add watcher to the current previous, hence
        // continue looping
        if (Futures.getUnchecked(exists) != null) {
          setWatcher = true;
          break;
        }
      }
    }

    if (!setWatcher) {
      // we are owners of a lock, just return
      return Futures.immediateFuture(true);
    }

    // wait for lock to be released by previous owner
    return future;
  }

  public ListenableFuture<Boolean> release() {
    if (lockNode == null) {
      return Futures.immediateFuture(false);
    }
    // if we hold a lock, we release it by deleting the node
    // todo: check that we still hold the lock?
    final SettableFuture<Boolean> unlocked = SettableFuture.create();
    Futures.addCallback(zkClient.delete(lockNode), new FutureCallback<String>() {
      @Override
      public void onSuccess(String result) {
        unlocked.set(true);
      }

      @Override
      public void onFailure(Throwable t) {
        // client should know about this
        throw Throwables.propagate(t);
      }
    });
    return unlocked;
  }

  private boolean isOwnerOfLock() {
    if (lockNode == null) {
      return false;
    }

    return Futures.getUnchecked(zkClient.exists(lockNode)) != null;
  }
}
