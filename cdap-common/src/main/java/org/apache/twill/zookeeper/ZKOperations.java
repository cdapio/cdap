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
package org.apache.twill.zookeeper;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.zookeeper.SettableOperationFuture;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;

/**
 * Collection of helper methods for common operations that usually needed when interacting with ZooKeeper.
 */
public final class ZKOperations {

  private static final Logger LOG = LoggerFactory.getLogger(ZKOperations.class);

  /**
   * Represents a ZK operation updates callback.
   * @param <T> Type of updated data.
   */
  public interface Callback<T> {
    void updated(T data);
  }

  /**
   * Interface for defining callback method to receive node data updates.
   */
  public interface DataCallback extends Callback<NodeData> {
    /**
     * Invoked when data of the node changed.
     * @param nodeData New data of the node, or {@code null} if the node has been deleted.
     */
    @Override
    void updated(NodeData nodeData);
  }

  /**
   * Interface for defining callback method to receive children nodes updates.
   */
  public interface ChildrenCallback extends Callback<NodeChildren> {
    @Override
    void updated(NodeChildren nodeChildren);
  }

  private interface Operation<T> {
    ZKClient getZKClient();

    OperationFuture<T> exec(String path, Watcher watcher);
  }

  /**
   * Watch for data changes of the given path. The callback will be triggered whenever changes has been
   * detected. Note that the callback won't see every single changes, as that's not the guarantee of ZooKeeper.
   * If the node doesn't exists, it will watch for its creation then starts watching for data changes.
   * When the node is deleted afterwards,
   *
   * @param zkClient The {@link ZKClient} for the operation
   * @param path Path to watch
   * @param callback Callback to be invoked when data changes is detected.
   * @return A {@link Cancellable} to cancel the watch.
   */
  public static Cancellable watchData(final ZKClient zkClient, final String path, final DataCallback callback) {
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    watchChanges(new Operation<NodeData>() {

      @Override
      public ZKClient getZKClient() {
        return zkClient;
      }

      @Override
      public OperationFuture<NodeData> exec(String path, Watcher watcher) {
        return zkClient.getData(path, watcher);
      }
    }, path, callback, cancelled);

    return new Cancellable() {
      @Override
      public void cancel() {
        cancelled.set(true);
      }
    };
  }

  public static ListenableFuture<String> watchDeleted(final ZKClient zkClient, final String path) {
    SettableFuture<String> completion = SettableFuture.create();
    watchDeleted(zkClient, path, completion);
    return completion;
  }

  public static void watchDeleted(final ZKClient zkClient, final String path,
                                  final SettableFuture<String> completion) {

    Futures.addCallback(zkClient.exists(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!completion.isDone()) {
          if (event.getType() == Event.EventType.NodeDeleted) {
            completion.set(path);
          } else {
            watchDeleted(zkClient, path, completion);
          }
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result == null) {
          completion.set(path);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        completion.setException(t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  public static Cancellable watchChildren(final ZKClient zkClient, String path, ChildrenCallback callback) {
    final AtomicBoolean cancelled = new AtomicBoolean(false);
    watchChanges(new Operation<NodeChildren>() {

      @Override
      public ZKClient getZKClient() {
        return zkClient;
      }

      @Override
      public OperationFuture<NodeChildren> exec(String path, Watcher watcher) {
        return zkClient.getChildren(path, watcher);
      }
    }, path, callback, cancelled);

    return new Cancellable() {
      @Override
      public void cancel() {
        cancelled.set(true);
      }
    };
  }

  /**
   * Returns a new {@link OperationFuture} that the result will be the same as the given future, except that when
   * the source future is having an exception matching the giving exception type, the errorResult will be set
   * in to the returned {@link OperationFuture}.
   * @param future The source future.
   * @param exceptionType Type of {@link KeeperException} to be ignored.
   * @param errorResult Object to be set into the resulting future on a matching exception.
   * @param <V> Type of the result.
   * @return A new {@link OperationFuture}.
   */
  public static <V> OperationFuture<V> ignoreError(OperationFuture<V> future,
                                                   final Class<? extends KeeperException> exceptionType,
                                                   final V errorResult) {
    final SettableOperationFuture<V> resultFuture = SettableOperationFuture.create(future.getRequestPath(),
                                                                                   Threads.SAME_THREAD_EXECUTOR);

    Futures.addCallback(future, new FutureCallback<V>() {
      @Override
      public void onSuccess(V result) {
        resultFuture.set(result);
      }

      @Override
      public void onFailure(Throwable t) {
        if (exceptionType.isAssignableFrom(t.getClass())) {
          resultFuture.set(errorResult);
        } else if (t instanceof CancellationException) {
          resultFuture.cancel(true);
        } else {
          resultFuture.setException(t);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    return resultFuture;
  }

  /**
   * Deletes the given path recursively. The delete method will keep running until the given path is successfully
   * removed, which means if there are new node created under the given path while deleting, they'll get deleted
   * again.  If there is {@link KeeperException} during the deletion other than
   * {@link KeeperException.NotEmptyException} or {@link KeeperException.NoNodeException},
   * the exception would be reflected in the result future and deletion process will stop,
   * leaving the given path with intermediate state.
   *
   * @param path The path to delete.
   * @return An {@link OperationFuture} that will be completed when the given path is deleted or bailed due to
   *         exception.
   */
  public static OperationFuture<String> recursiveDelete(final ZKClient zkClient, final String path) {
    final SettableOperationFuture<String> resultFuture =
      SettableOperationFuture.create(path, Threads.SAME_THREAD_EXECUTOR);

    // Try to delete the given path.
    Futures.addCallback(zkClient.delete(path), new FutureCallback<String>() {
      private final FutureCallback<String> deleteCallback = this;

      @Override
      public void onSuccess(String result) {
        // Path deleted successfully. Operation done.
        resultFuture.set(result);
      }

      @Override
      public void onFailure(Throwable t) {
        // Failed to delete the given path
        if (!(t instanceof KeeperException.NotEmptyException || t instanceof KeeperException.NoNodeException)) {
          // For errors other than NotEmptyException, treat the operation as failed.
          resultFuture.setException(t);
          return;
        }

        // If failed because of NotEmptyException, get the list of children under the given path
        Futures.addCallback(zkClient.getChildren(path), new FutureCallback<NodeChildren>() {

          @Override
          public void onSuccess(NodeChildren result) {
            // Delete all children nodes recursively.
            final List<OperationFuture<String>> deleteFutures = Lists.newLinkedList();
            for (String child :result.getChildren()) {
              deleteFutures.add(recursiveDelete(zkClient, path + "/" + child));
            }

            // When deletion of all children succeeded, delete the given path again.
            Futures.successfulAsList(deleteFutures).addListener(new Runnable() {
              @Override
              public void run() {
                for (OperationFuture<String> deleteFuture : deleteFutures) {
                  try {
                    // If any exception when deleting children, treat the operation as failed.
                    deleteFuture.get();
                  } catch (Exception e) {
                    resultFuture.setException(e.getCause());
                  }
                }
                Futures.addCallback(zkClient.delete(path), deleteCallback, Threads.SAME_THREAD_EXECUTOR);
              }
            }, Threads.SAME_THREAD_EXECUTOR);
          }

          @Override
          public void onFailure(Throwable t) {
            // If failed to get list of children, treat the operation as failed.
            resultFuture.setException(t);
          }
        }, Threads.SAME_THREAD_EXECUTOR);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    return resultFuture;
  }

  /**
   * Creates a ZK node of the given path. If the node already exists, deletion of the node (recursively) will happen
   * and the creation will be retried.
   */
  public static OperationFuture<String> createDeleteIfExists(final ZKClient zkClient, final String path,
                                                             @Nullable final byte[] data, final CreateMode createMode,
                                                             final boolean createParent, final ACL...acls) {
    final SettableOperationFuture<String> resultFuture = SettableOperationFuture.create(path,
                                                                                        Threads.SAME_THREAD_EXECUTOR);
    final List<ACL> createACLs = acls.length == 0 ? ZooDefs.Ids.OPEN_ACL_UNSAFE : Arrays.asList(acls);
    createNode(zkClient, path, data, createMode, createParent, createACLs, new FutureCallback<String>() {

      final FutureCallback<String> createCallback = this;

      @Override
      public void onSuccess(String result) {
        // Create succeeded, just set the result to the resultFuture
        resultFuture.set(result);
      }

      @Override
      public void onFailure(final Throwable createFailure) {
        // If create failed not because of the NodeExistsException, just set the exception to the result future
        if (!(createFailure instanceof KeeperException.NodeExistsException)) {
          resultFuture.setException(createFailure);
          return;
        }

        // Try to delete the path
        LOG.info("Node {}{} already exists. Deleting it and retry creation", zkClient.getConnectString(), path);
        Futures.addCallback(recursiveDelete(zkClient, path), new FutureCallback<String>() {
          @Override
          public void onSuccess(String result) {
            // If delete succeeded, perform the creation again.
            createNode(zkClient, path, data, createMode, createParent, createACLs, createCallback);
          }

          @Override
          public void onFailure(Throwable t) {
            // If deletion failed because of NoNodeException, fail the result operation future
            if (!(t instanceof KeeperException.NoNodeException)) {
              createFailure.addSuppressed(t);
              resultFuture.setException(createFailure);
              return;
            }

            // If can't delete because the node no longer exists, just go ahead and recreate the node
            createNode(zkClient, path, data, createMode, createParent, createACLs, createCallback);
          }
        }, Threads.SAME_THREAD_EXECUTOR);
      }
    });

    return resultFuture;
  }

  /**
   * Private helper method to create a ZK node based on the parameter. The result of the creation is always
   * communicate via the provided {@link FutureCallback}.
   */
  private static void createNode(ZKClient zkClient, String path, @Nullable byte[] data,
                                 CreateMode createMode, boolean createParent,
                                 Iterable<ACL> acls, FutureCallback<String> callback) {
    Futures.addCallback(zkClient.create(path, data, createMode, createParent, acls),
                        callback, Threads.SAME_THREAD_EXECUTOR);
  }

  /**
   * Watch for the given path until it exists.
   * @param zkClient The {@link ZKClient} to use.
   * @param path A ZooKeeper path to watch for existent.
   */
  private static void watchExists(final ZKClient zkClient, final String path, final SettableFuture<String> completion) {
    Futures.addCallback(zkClient.exists(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!completion.isDone()) {
          watchExists(zkClient, path, completion);
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        if (result != null) {
          completion.set(path);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        completion.setException(t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private static <T> void watchChanges(final Operation<T> operation, final String path,
                                       final Callback<T> callback, final AtomicBoolean cancelled) {
    Futures.addCallback(operation.exec(path, new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (!cancelled.get()) {
          watchChanges(operation, path, callback, cancelled);
        }
      }
    }), new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        if (!cancelled.get()) {
          callback.updated(result);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (t instanceof KeeperException && ((KeeperException) t).code() == KeeperException.Code.NONODE) {
          final SettableFuture<String> existCompletion = SettableFuture.create();
          existCompletion.addListener(new Runnable() {
            @Override
            public void run() {
              try {
                if (!cancelled.get()) {
                  watchChanges(operation, existCompletion.get(), callback, cancelled);
                }
              } catch (Exception e) {
                LOG.error("Failed to watch children for path " + path, e);
              }
            }
          }, Threads.SAME_THREAD_EXECUTOR);
          watchExists(operation.getZKClient(), path, existCompletion);
          return;
        }
        LOG.error("Failed to watch data for path " + path + " " + t, t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private ZKOperations() {
  }
}
