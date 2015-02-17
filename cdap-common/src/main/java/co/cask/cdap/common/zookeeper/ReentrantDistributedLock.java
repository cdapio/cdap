/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.common.zookeeper;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

/**
 * A reentrant distributed lock implementation that uses ZooKeeper using the receipt described in
 *
 * http://zookeeper.apache.org/doc/trunk/recipes.html#sc_recipes_Locks
 */
public final class ReentrantDistributedLock implements Lock {

  private static final Logger LOG = LoggerFactory.getLogger(ReentrantDistributedLock.class);

  private final ZKClient zkClient;
  private final String path;
  private final ThreadLocal<String> localLockNode;
  private final ReentrantLock lock;

  /**
   * Creates a distributed lock instance.
   *
   * @param zkClient the {@link ZKClient} to interact with the ZooKeeper used for the lock coordination
   * @param path the path in ZooKeeper where the lock coordination happens
   */
  public ReentrantDistributedLock(ZKClient zkClient, String path) {
    this.zkClient = zkClient;
    this.path = path.startsWith("/") ? path : "/" + path;
    this.localLockNode = new ThreadLocal<String>();
    this.lock = new ReentrantLock();
  }

  @Override
  public void lock() {
    lock.lock();
    try {
      acquire(false, true);
    } catch (Exception e) {
      lock.unlock();
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    lock.lockInterruptibly();
    try {
      acquire(true, true);
    } catch (Exception e) {
      lock.unlock();
      Throwables.propagateIfInstanceOf(e, InterruptedException.class);
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean tryLock() {
    if (!lock.tryLock()) {
      return false;
    }
    try {
      if (acquire(false, false)) {
        return true;
      }
      lock.unlock();
      return false;
    } catch (Exception e) {
      lock.unlock();
      throw Throwables.propagate(e);
    }
  }

  @Override
  public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
    long startTime = System.nanoTime();
    if (!lock.tryLock(time, unit)) {
      return false;
    }
    long timeoutNano = unit.toNanos(time) - (System.nanoTime() - startTime);
    try {
      if (acquire(true, true, timeoutNano, TimeUnit.NANOSECONDS)) {
        return true;
      }
      lock.unlock();
      return false;
    } catch (ExecutionException e) {
      lock.unlock();
      throw Throwables.propagate(e.getCause());
    } catch (TimeoutException e) {
      lock.unlock();
      return false;
    }
  }

  @Override
  public void unlock() {
    if (!lock.isHeldByCurrentThread()) {
      throw new IllegalStateException("Cannot unlock without holding a lock by thread " + Thread.currentThread());
    }

    try {
      if (lock.getHoldCount() == 1) {
        // If it is the last lock entry for this thread, remove the zk node as well.
        try {
          Uninterruptibles.getUninterruptibly(zkClient.delete(localLockNode.get()));
        } catch (ExecutionException e) {
          throw Throwables.propagate(e.getCause());
        } finally {
          localLockNode.remove();
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Currently not supported and will always throw {@link UnsupportedOperationException}.
   */
  @Override
  public Condition newCondition() {
    // TODO: Add the support of it when needed
    throw new UnsupportedOperationException("Condition not supported.");
  }

  /**
   * Acquires a distributed lock through ZooKeeper. It's the same as calling
   * {@link #acquire(boolean, boolean, long, TimeUnit)} with {@link Long#MAX_VALUE} as timeout.
   */
  private boolean acquire(boolean interruptible, boolean waitForLock) throws InterruptedException, ExecutionException {
    try {
      return acquire(interruptible, waitForLock, Long.MAX_VALUE, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      // Should never happen
      throw Throwables.propagate(e);
    }
  }

  /**
   * Acquires a distributed lock through ZooKeeper.
   *
   * @param interruptible true if acquisition of lock can be interrupted
   * @param waitForLock true if wants to wait for the lock when not able to acquire it
   * @param timeout time to wait for the lock before giving up
   * @param unit unit for the timeout
   * @throws InterruptedException if {@code interruptible} is set to {@code true} and the current thread is interrupted
   *                              while acquiring the lock
   * @throws ExecutionException if there is failure while trying to acquire the lock
   */
  private boolean acquire(boolean interruptible,
                          final boolean waitForLock,
                          long timeout,
                          TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    Preconditions.checkState(lock.isHeldByCurrentThread(), "Not owner of local lock.");
    if (lock.getHoldCount() > 1) {
      // Already owner of the lock, simply return.
      return true;
    }

    // Use a Future to help deal with different variants of locking
    // (lock, lockInterruptibly, tryLock, tryLock with timeout)
    // When the completion future is completed successfully, it means the lock is acquired and the future contains
    // the ZK node path to the ephemeral node that is representing this lock.
    // If it is failed, it means there is exception while trying to acquire the lock
    // If it is cancelled, it means to abort the acquisition logic (due to timeout / interrupt).
    final SettableFuture<String> completion = SettableFuture.create();

    // Step 1. Create a ephemeral sequential node
    final String guid = UUID.randomUUID().toString();
    final String lockPath = String.format("%s/%s-", path, guid);
    OperationFuture<String> future = zkClient.create(lockPath, null, CreateMode.EPHEMERAL_SEQUENTIAL, true);

    Futures.addCallback(future, new FutureCallback<String>() {
      @Override
      public void onSuccess(final String lockNode) {
        // If lock failed due to whatever reason, delete the lock node.
        deleteNodeOnFailure(completion, lockNode);

        // If the lock is completed (mainly due to cancellation), simply abort the lock acquisition logic.
        if (completion.isDone()) {
          return;
        }

        // Step 2-5. Try to determine who is the lock owner and watch for ZK node changes if itself is not the owner.
        doAcquire(completion, waitForLock, guid, lockNode);
      }

      @Override
      public void onFailure(Throwable t) {
        if (t instanceof KeeperException.ConnectionLossException) {
          // Ignore connection exception in create. Going to handle it in next step.
          // See the ZK receipt for details about the possible failure situation that can cause this.
          doAcquire(completion, waitForLock, guid, null);
        } else {
          LOG.error("Exception raised when creating lock node at {}", lockPath, t);
          completion.setException(t);
        }
      }
    });

    // Gets the result from the completion
    try {
      if (interruptible) {
        localLockNode.set(completion.get(timeout, unit));
      } else {
        localLockNode.set(Uninterruptibles.getUninterruptibly(completion, timeout, unit));
      }
      return true;
    } catch (InterruptedException e) {
      completion.cancel(true);
      throw e;
    } catch (TimeoutException e) {
      completion.cancel(true);
      throw e;
    } catch (CancellationException e) {
      // If the completion get cancelled, meaning the lock acquisition is aborted.
      return false;
    }
  }

  /**
   * Executes the lock acquisition process. This corresponds to step 2-5 of the distributed lock receipt.
   */
  private void doAcquire(final SettableFuture<String> completion, final boolean waitForLock,
                         final String guid, @Nullable final String lockPath) {
    // Step 2. Get all children under the lock parent.
    Futures.addCallback(zkClient.getChildren(path), new FutureCallback<NodeChildren>() {

      @Override
      public void onSuccess(NodeChildren children) {
        // Find the lock node in case the creation step failed by matching the guid
        // See "Recoverable Errors and the GUID" in the ZooKeeper guide
        final String lockNode = lockPath == null ? findLockNode(children.getChildren(), guid) : lockPath;
        if (lockNode == null) {
          // If not able to find the lock node, fail the locking procedure.
          completion.setException(new IllegalStateException("Failed to acquire lock").fillInStackTrace());
          return;
        }

        if (lockPath == null) {
          // If lock node was not determined in step 1 due to connection loss exception, need to add the
          // node deletion handler in here after the actual lockNode is determined.
          deleteNodeOnFailure(completion, lockNode);
        }

        // Find the node to watch, which is the one with the largest id that is smaller than currentId
        // If the current id is the smallest one, nodeToWatch will be null
        final String nodeToWatch = findNodeToWatch(children, lockNode, guid);

        // Step 3a. lockNode is the lowest, hence this become lock owner.
        if (nodeToWatch == null) {
          // Acquired lock
          completion.set(lockNode);
        } else if (!waitForLock) {
          // This is for the case of tryLock() without timeout.
          completion.cancel(true);
        }
        // If the lock acquisition is completed, due to whatever reason, we don't need to watch for any other nodes
        if (completion.isDone()) {
          return;
        }

        // Step 3b and 4. See if the the next lowest sequence ID exists. If it does, leave a watch
        // Use getData() instead of exists() to avoid leaking Watcher resources (if the node is gone, there will
        // be a watch left on the ZK server if exists() is used).
        OperationFuture<NodeData> getDataFuture = zkClient.getData(nodeToWatch, new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            if (!completion.isDone()) {
              // If the watching node changed, go to step 2.
              doAcquire(completion, waitForLock, guid, lockNode);
            }
          }
        });

        // Step 5. Depends on the exists call result, either go to step 2 if the nodeToWatch is gone or just
        // let the watcher to trigger step 2 when there is change to the nodeToWatch.
        Futures.addCallback(getDataFuture, new FutureCallback<NodeData>() {
          @Override
          public void onSuccess(NodeData nodeData) {
            // No-op
          }

          @Override
          public void onFailure(Throwable t) {
            // See if the failure is due to node not exists. If that's the case, go to step 2.
            if (t instanceof KeeperException.NoNodeException && !completion.isDone()) {
              doAcquire(completion, waitForLock, guid, lockNode);
            } else {
              // If failed due to something else, fail the lock acquisition.
              completion.setException(t);
            }
          }
        });
      }

      @Override
      public void onFailure(Throwable t) {
        if (lockPath != null) {
          completion.setException(t);
        } else {
          doAcquire(completion, waitForLock, guid, null);
        }
      }
    });
  }

  /**
   * Deletes the given node if the given future failed.
   */
  private void deleteNodeOnFailure(final ListenableFuture<?> future, final String node) {
    future.addListener(new Runnable() {
      @Override
      public void run() {
        try {
          future.get();
        } catch (Exception e) {
          zkClient.delete(node);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  /**
   * Find the maximum node that is smaller than node of the given lockPath.
   *
   * @return the node found or {@code null} if no such node exist
   */
  private String findNodeToWatch(NodeChildren children, String lockPath, String guid) {
    // Lock path is "path/guid-id"
    int guidLen = guid.length();
    int id = Integer.parseInt(lockPath.substring(path.length() + guidLen + 2));

    String nodeToWatch = null;
    int maxOfMins = Integer.MIN_VALUE;
    for (String node : children.getChildren()) {
      int nodeId = Integer.parseInt(node.substring(guidLen + 1));
      if (nodeId < id && nodeId > maxOfMins) {
        maxOfMins = nodeId;
        nodeToWatch = path + "/" + node;
      }
    }
    return nodeToWatch;
  }

  /**
   * Finds the node path that matches the given prefix.
   */
  private String findLockNode(Iterable<String> nodes, String prefix) {
    for (String child : nodes) {
      if (child.startsWith(prefix)) {
        return path + "/" + child;
      }
    }
    return null;
  }
}
