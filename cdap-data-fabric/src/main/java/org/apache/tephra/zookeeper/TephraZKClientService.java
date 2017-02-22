/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.zookeeper;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.zookeeper.SettableOperationFuture;
import org.apache.twill.zookeeper.ACLData;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClientService;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * The implementation of {@link ZKClientService}.
 */
public class TephraZKClientService extends AbstractService implements ZKClientService, Watcher {

  private static final Logger LOG = LoggerFactory.getLogger(TephraZKClientService.class);

  private final String zkStr;
  private final int sessionTimeout;
  private final List<Watcher> connectionWatchers;
  private final Multimap<String, byte[]> authInfos;
  private final AtomicReference<ZooKeeper> zooKeeper;
  private final Runnable stopTask;
  private ExecutorService eventExecutor;

  /**
   * Create a new instance.
   * @param zkStr zookeper connection string
   * @param sessionTimeout timeout in milliseconds
   * @param connectionWatcher watcher to set
   * @param authInfos authorization bytes
   */
  public TephraZKClientService(String zkStr, int sessionTimeout,
                               Watcher connectionWatcher, Multimap<String, byte[]> authInfos) {
    this.zkStr = zkStr;
    this.sessionTimeout = sessionTimeout;
    this.connectionWatchers = new CopyOnWriteArrayList<>();
    this.authInfos = copyAuthInfo(authInfos);
    addConnectionWatcher(connectionWatcher);

    this.zooKeeper = new AtomicReference<>();
    this.stopTask = createStopTask();
  }

  @Override
  public Long getSessionId() {
    ZooKeeper zk = zooKeeper.get();
    return zk == null ? null : zk.getSessionId();
  }

  @Override
  public String getConnectString() {
    return zkStr;
  }

  @Override
  public Cancellable addConnectionWatcher(final Watcher watcher) {
    if (watcher == null) {
      return new Cancellable() {
        @Override
        public void cancel() {
          // No-op
        }
      };
    }

    // Invocation of connection watchers are already done inside the event thread,
    // hence no need to wrap the watcher again.
    connectionWatchers.add(watcher);
    return new Cancellable() {
      @Override
      public void cancel() {
        connectionWatchers.remove(watcher);
      }
    };
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode) {
    return create(path, data, createMode, true);
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data,
                                        CreateMode createMode, boolean createParent) {
    return create(path, data, createMode, createParent, ZooDefs.Ids.OPEN_ACL_UNSAFE);
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data,
                                        CreateMode createMode, Iterable<ACL> acl) {
    return create(path, data, createMode, true, acl);
  }

  @Override
  public OperationFuture<Stat> exists(String path) {
    return exists(path, null);
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path) {
    return getChildren(path, null);
  }

  @Override
  public OperationFuture<NodeData> getData(String path) {
    return getData(path, null);
  }

  @Override
  public OperationFuture<Stat> setData(String path, byte[] data) {
    return setData(path, data, -1);
  }

  @Override
  public OperationFuture<String> delete(String path) {
    return delete(path, -1);
  }

  @Override
  public OperationFuture<Stat> setACL(String path, Iterable<ACL> acl) {
    return setACL(path, acl, -1);
  }

  @Override
  public OperationFuture<String> create(String path, @Nullable byte[] data, CreateMode createMode,
                                        boolean createParent, Iterable<ACL> acl) {
    return doCreate(path, data, createMode, createParent, ImmutableList.copyOf(acl), false);
  }

  private OperationFuture<String> doCreate(final String path,
                                           @Nullable final byte[] data,
                                           final CreateMode createMode,
                                           final boolean createParent,
                                           final List<ACL> acl,
                                           final boolean ignoreNodeExists) {
    final SettableOperationFuture<String> createFuture = SettableOperationFuture.create(path, eventExecutor);
    getZooKeeper().create(path, data, acl, createMode, Callbacks.STRING, createFuture);
    if (!createParent) {
      return createFuture;
    }

    // If create parent is request, return a different future
    final SettableOperationFuture<String> result = SettableOperationFuture.create(path, eventExecutor);
    // Watch for changes in the original future
    Futures.addCallback(createFuture, new FutureCallback<String>() {
      @Override
      public void onSuccess(String path) {
        // Propagate if creation was successful
        result.set(path);
      }

      @Override
      public void onFailure(Throwable t) {
        // See if the failure can be handled
        if (updateFailureResult(t, result, path, ignoreNodeExists)) {
          return;
        }
        // Create the parent node
        String parentPath = getParent(path);
        if (parentPath.isEmpty()) {
          result.setException(t);
          return;
        }
        // Watch for parent creation complete. Parent is created with the unsafe ACL.
        Futures.addCallback(doCreate(parentPath, null, CreateMode.PERSISTENT,
                                     true, ZooDefs.Ids.OPEN_ACL_UNSAFE, true), new FutureCallback<String>() {
          @Override
          public void onSuccess(String parentPath) {
            // Create the requested path again
            Futures.addCallback(
              doCreate(path, data, createMode, false, acl, ignoreNodeExists), new FutureCallback<String>() {
                @Override
                public void onSuccess(String pathResult) {
                  result.set(pathResult);
                }

                @Override
                public void onFailure(Throwable t) {
                  // handle the failure
                  updateFailureResult(t, result, path, ignoreNodeExists);
                }
              });
          }

          @Override
          public void onFailure(Throwable t) {
            result.setException(t);
          }
        });
      }

      /**
       * Updates the result future based on the given {@link Throwable}.
       * @param t Cause of the failure
       * @param result Future to be updated
       * @param path Request path for the operation
       * @return {@code true} if it is a failure, {@code false} otherwise.
       */
      private boolean updateFailureResult(Throwable t, SettableOperationFuture<String> result,
                                          String path, boolean ignoreNodeExists) {
        // Propagate if there is error
        if (!(t instanceof KeeperException)) {
          result.setException(t);
          return true;
        }
        KeeperException.Code code = ((KeeperException) t).code();
        // Node already exists, simply return success if it allows for ignoring node exists (for parent node creation).
        if (ignoreNodeExists && code == KeeperException.Code.NODEEXISTS) {
          // The requested path could be used because it only applies to non-sequential node
          result.set(path);
          return false;
        }
        if (code != KeeperException.Code.NONODE) {
          result.setException(t);
          return true;
        }
        return false;
      }

      /**
       * Gets the parent of the given path.
       * @param path Path for computing its parent
       * @return Parent of the given path, or empty string if the given path is the root path already.
       */
      private String getParent(String path) {
        String parentPath = path.substring(0, path.lastIndexOf('/'));
        return (parentPath.isEmpty() && !"/".equals(path)) ? "/" : parentPath;
      }
    });

    return result;
  }

  @Override
  public OperationFuture<Stat> exists(String path, Watcher watcher) {
    SettableOperationFuture<Stat> result = SettableOperationFuture.create(path, eventExecutor);
    getZooKeeper().exists(path, wrapWatcher(watcher), Callbacks.STAT_NONODE, result);
    return result;
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(String path, Watcher watcher) {
    SettableOperationFuture<NodeChildren> result = SettableOperationFuture.create(path, eventExecutor);
    getZooKeeper().getChildren(path, wrapWatcher(watcher), Callbacks.CHILDREN, result);
    return result;
  }

  @Override
  public OperationFuture<NodeData> getData(String path, Watcher watcher) {
    SettableOperationFuture<NodeData> result = SettableOperationFuture.create(path, eventExecutor);
    getZooKeeper().getData(path, wrapWatcher(watcher), Callbacks.DATA, result);

    return result;
  }

  @Override
  public OperationFuture<Stat> setData(String dataPath, byte[] data, int version) {
    SettableOperationFuture<Stat> result = SettableOperationFuture.create(dataPath, eventExecutor);
    getZooKeeper().setData(dataPath, data, version, Callbacks.STAT, result);
    return result;
  }

  @Override
  public OperationFuture<String> delete(String deletePath, int version) {
    SettableOperationFuture<String> result = SettableOperationFuture.create(deletePath, eventExecutor);
    getZooKeeper().delete(deletePath, version, Callbacks.VOID, result);
    return result;
  }

  @Override
  public OperationFuture<ACLData> getACL(String path) {
    SettableOperationFuture<ACLData> result = SettableOperationFuture.create(path, eventExecutor);
    getZooKeeper().getACL(path, new Stat(), Callbacks.ACL, result);
    return result;
  }

  @Override
  public OperationFuture<Stat> setACL(String path, Iterable<ACL> acl, int version) {
    SettableOperationFuture<Stat> result = SettableOperationFuture.create(path, eventExecutor);
    getZooKeeper().setACL(path, ImmutableList.copyOf(acl), version, Callbacks.STAT, result);
    return result;
  }

  @Override
  public Supplier<ZooKeeper> getZooKeeperSupplier() {
    return new Supplier<ZooKeeper>() {
      @Override
      public ZooKeeper get() {
        return getZooKeeper();
      }
    };
  }

  @Override
  protected void doStart() {
    // A single thread executor for all events
    ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                                                         new LinkedBlockingQueue<Runnable>(),
                                                         Threads.createDaemonThreadFactory("zk-client-EventThread"));
    // Just discard the execution if the executor is closed
    executor.setRejectedExecutionHandler(new ThreadPoolExecutor.DiscardPolicy());
    eventExecutor = executor;

    try {
      zooKeeper.set(createZooKeeper());
    } catch (IOException e) {
      notifyFailed(e);
    }
  }

  @Override
  protected void doStop() {
    // Submit a task to the executor to make sure all pending events in the executor are fired before
    // transiting this Service into STOPPED state
    eventExecutor.submit(stopTask);
    eventExecutor.shutdown();
  }

  /**
   * @return Current {@link ZooKeeper} client.
   */
  private ZooKeeper getZooKeeper() {
    ZooKeeper zk = zooKeeper.get();
    Preconditions.checkArgument(zk != null, "Not connected to zooKeeper.");
    return zk;
  }

  /**
   * Wraps the given watcher to be called from the event executor.
   * @param watcher Watcher to be wrapped
   * @return The wrapped Watcher
   */
  private Watcher wrapWatcher(final Watcher watcher) {
    if (watcher == null) {
      return null;
    }
    return new Watcher() {
      @Override
      public void process(final WatchedEvent event) {
        if (eventExecutor.isShutdown()) {
          LOG.debug("Already shutdown. Discarding event: {}", event);
          return;
        }
        eventExecutor.execute(new Runnable() {
          @Override
          public void run() {
            try {
              watcher.process(event);
            } catch (Throwable t) {
              LOG.error("Watcher throws exception.", t);
            }
          }
        });
      }
    };
  }

  /**
   * Creates a deep copy of the given authInfos multimap.
   */
  private Multimap<String, byte[]> copyAuthInfo(Multimap<String, byte[]> authInfos) {
    Multimap<String, byte[]> result = ArrayListMultimap.create();

    for (Map.Entry<String, byte[]> entry : authInfos.entries()) {
      byte[] info = entry.getValue();
      result.put(entry.getKey(), info == null ? null : Arrays.copyOf(info, info.length));
    }

    return result;
  }

  @Override
  public void process(WatchedEvent event) {
    State state = state();
    if (state == State.TERMINATED || state == State.FAILED) {
      return;
    }

    try {
      if (event.getState() == Event.KeeperState.SyncConnected && state == State.STARTING) {
        LOG.debug("Connected to ZooKeeper: {}", zkStr);
        notifyStarted();
        return;
      }
      if (event.getState() == Event.KeeperState.Expired) {
        LOG.info("ZooKeeper session expired: {}", zkStr);

        // When connection expired, simply reconnect again
        if (state != State.RUNNING) {
          return;
        }
        eventExecutor.submit(new Runnable() {
          @Override
          public void run() {
            // Only reconnect if the current state is running
            if (state() != State.RUNNING) {
              return;
            }
            try {
              LOG.info("Reconnect to ZooKeeper due to expiration: {}", zkStr);
              closeZooKeeper(zooKeeper.getAndSet(createZooKeeper()));
            } catch (IOException e) {
              notifyFailed(e);
            }
          }
        });
      }
    } finally {
      if (event.getType() == Event.EventType.None) {
        for (Watcher connectionWatcher : connectionWatchers) {
          connectionWatcher.process(event);
        }
      }
    }
  }

  /**
   * Creates a new ZooKeeper connection.
   */
  private ZooKeeper createZooKeeper() throws IOException {
    ZooKeeper zk = new ZooKeeper(zkStr, sessionTimeout, wrapWatcher(this));
    for (Map.Entry<String, byte[]> authInfo : authInfos.entries()) {
      zk.addAuthInfo(authInfo.getKey(), authInfo.getValue());
    }
    return zk;
  }

  /**
   * Closes the given {@link ZooKeeper} if it is not null. If there is InterruptedException,
   * it will get logged.
   */
  private void closeZooKeeper(@Nullable ZooKeeper zk) {
    try {
      if (zk != null) {
        zk.close();
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted when closing ZooKeeper", e);
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Creates a {@link Runnable} task that will get executed in the event executor for transiting this
   * Service into STOPPED state.
   */
  private Runnable createStopTask() {
    return new Runnable() {
      @Override
      public void run() {
        try {
          // Close the ZK connection in this task will make sure if there is ZK connection created
          // after doStop() was called but before this task has been executed is also closed.
          // It is possible to happen when the following sequence happens:
          //
          // 1. session expired, hence the expired event is triggered
          // 2. The reconnect task executed. With Service.state() == RUNNING, it creates a new ZK client
          // 3. Service.stop() gets called, Service.state() changed to STOPPING
          // 4. The new ZK client created from the reconnect thread update the zooKeeper with the new one
          closeZooKeeper(zooKeeper.getAndSet(null));
          notifyStopped();
        } catch (Exception e) {
          notifyFailed(e);
        }
      }
    };
  }

  /**
   * Collection of generic callbacks that simply reflect results into OperationFuture.
   */
  private static final class Callbacks {
    static final AsyncCallback.StringCallback STRING = new AsyncCallback.StringCallback() {
      @Override
      @SuppressWarnings("unchecked")
      public void processResult(int rc, String path, Object ctx, String name) {
        SettableOperationFuture<String> result = (SettableOperationFuture<String>) ctx;
        KeeperException.Code code = KeeperException.Code.get(rc);
        if (code == KeeperException.Code.OK) {
          result.set((name == null || name.isEmpty()) ? path : name);
          return;
        }
        result.setException(KeeperException.create(code, result.getRequestPath()));
      }
    };

    static final AsyncCallback.StatCallback STAT = new AsyncCallback.StatCallback() {
      @Override
      @SuppressWarnings("unchecked")
      public void processResult(int rc, String path, Object ctx, Stat stat) {
        SettableOperationFuture<Stat> result = (SettableOperationFuture<Stat>) ctx;
        KeeperException.Code code = KeeperException.Code.get(rc);
        if (code == KeeperException.Code.OK) {
          result.set(stat);
          return;
        }
        result.setException(KeeperException.create(code, result.getRequestPath()));
      }
    };

    /**
     * A stat callback that treats NONODE as success.
     */
    static final AsyncCallback.StatCallback STAT_NONODE = new AsyncCallback.StatCallback() {
      @Override
      @SuppressWarnings("unchecked")
      public void processResult(int rc, String path, Object ctx, Stat stat) {
        SettableOperationFuture<Stat> result = (SettableOperationFuture<Stat>) ctx;
        KeeperException.Code code = KeeperException.Code.get(rc);
        if (code == KeeperException.Code.OK || code == KeeperException.Code.NONODE) {
          result.set(stat);
          return;
        }
        result.setException(KeeperException.create(code, result.getRequestPath()));
      }
    };

    static final AsyncCallback.Children2Callback CHILDREN = new AsyncCallback.Children2Callback() {
      @Override
      @SuppressWarnings("unchecked")
      public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
        SettableOperationFuture<NodeChildren> result = (SettableOperationFuture<NodeChildren>) ctx;
        KeeperException.Code code = KeeperException.Code.get(rc);
        if (code == KeeperException.Code.OK) {
          result.set(new BasicNodeChildren(children, stat));
          return;
        }
        result.setException(KeeperException.create(code, result.getRequestPath()));
      }
    };

    static final AsyncCallback.DataCallback DATA = new AsyncCallback.DataCallback() {
      @Override
      @SuppressWarnings("unchecked")
      public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        SettableOperationFuture<NodeData> result = (SettableOperationFuture<NodeData>) ctx;
        KeeperException.Code code = KeeperException.Code.get(rc);
        if (code == KeeperException.Code.OK) {
          result.set(new BasicNodeData(data, stat));
          return;
        }
        result.setException(KeeperException.create(code, result.getRequestPath()));
      }
    };

    static final AsyncCallback.VoidCallback VOID = new AsyncCallback.VoidCallback() {
      @Override
      @SuppressWarnings("unchecked")
      public void processResult(int rc, String path, Object ctx) {
        SettableOperationFuture<String> result = (SettableOperationFuture<String>) ctx;
        KeeperException.Code code = KeeperException.Code.get(rc);
        if (code == KeeperException.Code.OK) {
          result.set(result.getRequestPath());
          return;
        }
        // Otherwise, it is an error
        result.setException(KeeperException.create(code, result.getRequestPath()));
      }
    };

    static final AsyncCallback.ACLCallback ACL = new AsyncCallback.ACLCallback() {
      @Override
      @SuppressWarnings("unchecked")
      public void processResult(int rc, String path, Object ctx, List<ACL> acl, Stat stat) {
        SettableOperationFuture<ACLData> result = (SettableOperationFuture<ACLData>) ctx;
        KeeperException.Code code = KeeperException.Code.get(rc);
        if (code == KeeperException.Code.OK) {
          result.set(new BasicACLData(acl, stat));
          return;
        }
        result.setException(KeeperException.create(code, result.getRequestPath()));
      }
    };
  }
}
