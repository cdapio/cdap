/*
 * Copyright © 2014-2016 Cask Data, Inc.
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

import co.cask.cdap.common.async.AsyncFunctions;
import co.cask.cdap.common.io.Codec;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Collection of common zk operations.
 *
 * NOTE: When this class is matured, we could move this into twill ZKOperations.
 */
public final class ZKExtOperations {

  /**
   * Attempts to create a persistent node with the given content. If creation failed because the node already
   * exists ({@link KeeperException.NodeExistsException}), the node will be set with the given content.
   * This method is suitable for cases where the node expected to be non-existed.
   *
   * @param zkClient The ZKClient to perform the operations.
   * @param path The path in ZK.
   * @param dataSupplier The supplier to provide the content to be set to the node. The supplier may get invoked
   *                     multiple times when the actual data is needed for creating or setting the content of
   *                     the given node. The supplier can be invoked from the caller thread as well as the
   *                     zookeeper event callback thread.
   * @param codec A {@link Codec} for serializing the data into byte array.
   * @param maxFailure Maximum number of times to try to create/set the content.
   * @param <T> Type of the data.
   * @return A {@link ListenableFuture} that will be completed when node is created or data is set. The future will
   *         fail if failed to create and to set the data. Calling {@link ListenableFuture#cancel(boolean)} has
   *         no effect.
   */
  public static <T> ListenableFuture<T> createOrSet(ZKClient zkClient, String path, Supplier<T> dataSupplier,
                                                    Codec<T> codec, int maxFailure) {
    return createOrSetWithRetry(true, zkClient, path, dataSupplier, codec, null, maxFailure);
  }

  /**
   * Attempts to create a persistent node with the given content. If creation failed because the node already
   * exists ({@link KeeperException.NodeExistsException}), the node will be set with the given content.
   * This method is suitable for cases where the node expected to be non-existed.
   *
   * @param zkClient The ZKClient to perform the operations.
   * @param path The path in ZK.
   * @param dataSupplier The supplier to provide the content to be set to the node. The supplier may get invoked
   *                     multiple times when the actual data is needed for creating or setting the content of
   *                     the given node. The supplier can be invoked from the caller thread as well as the
   *                     zookeeper event callback thread.
   * @param codec A {@link Codec} for serializing the data into byte array.
   * @param maxFailure Maximum number of times to try to create/set the content.
   * @param acls The access control list to set on the node, if it is created.
   * @param <T> Type of the data.
   * @return A {@link ListenableFuture} that will be completed when node is created or data is set. The future will
   *         fail if failed to create and to set the data. Calling {@link ListenableFuture#cancel(boolean)} has
   *         no effect.
   */
  public static <T> ListenableFuture<T> createOrSet(ZKClient zkClient, String path, Supplier<T> dataSupplier,
                                                    Codec<T> codec, int maxFailure, List<ACL> acls) {
    return createOrSetWithRetry(true, zkClient, path, dataSupplier, codec, acls, maxFailure);
  }

  /**
   * Attempts to set the content of the given node. If it failed due to node doesn't exist
   * ({@link KeeperException.NoNodeException}), a persistent node will be created with the given content.
   * This method is suitable for cases where the node is expected to be existed.
   *
   * @param zkClient The ZKClient to perform the operations.
   * @param path The path in ZK.
   * @param dataSupplier The supplier to provide the content to be set to the node. The supplier may get invoked
   *                     multiple times when the actual data is needed for creating or setting the content of
   *                     the given node. The supplier can be invoked from the caller thread as well as the
   *                     zookeeper event callback thread.
   * @param codec A {@link Codec} for serializing the data into byte array.
   * @param maxFailure Maximum number of times to try to create/set the content.
   * @param <T> Type of the data.
   * @return A {@link ListenableFuture} that will be completed when node is created or data is set. The future will
   *         fail if failed to create and to set the data. Calling {@link ListenableFuture#cancel(boolean)} has
   *         no effect.
   */
  public static <T> ListenableFuture<T> setOrCreate(ZKClient zkClient, String path, Supplier<T> dataSupplier,
                                                    Codec<T> codec, int maxFailure) {
    return createOrSetWithRetry(false, zkClient, path, dataSupplier, codec, null, maxFailure);
  }

  /**
   * Update the content of the given node. If the node doesn't exist, it will try to create the node. Same as calling
   *
   * {@link #updateOrCreate(ZKClient, String, Function, Codec, List)
   * updateOrCreate(zkClient, path, modifier, codec, null)}
   *
   * @see #updateOrCreate(ZKClient, String, Function, Codec, java.util.List)
   */
  public static <V> ListenableFuture<V> updateOrCreate(ZKClient zkClient, String path,
                                                       Function<V, V> modifier, Codec<V> codec) {
    return updateOrCreate(zkClient, path, modifier, codec, null);
  }

  /**
   * Update the content of the given node. If the node doesn't exist, it will try to create the node.
   * The modifier will be executed in the ZooKeeper callback thread, hence no blocking operation should be performed
   * in it. If blocking operation is needed, use the async version of this method.
   *
   * @see #updateOrCreate(ZKClient, String, AsyncFunction, Codec, java.util.List)
   */
  public static <V> ListenableFuture<V> updateOrCreate(ZKClient zkClient, String path,
                                                       Function<V, V> modifier, Codec<V> codec,
                                                       @Nullable List<ACL> createAcl) {
    SettableFuture<V> resultFuture = SettableFuture.create();
    AsyncFunction<V, V> asyncModifier = AsyncFunctions.asyncWrap(modifier);
    getAndSet(zkClient, path, asyncModifier, codec, resultFuture, createAcl);
    return resultFuture;
  }

  /**
   * Update the content of the given node. If the node doesn't exist, it will try to create the node. Same as calling
   *
   * {@link #updateOrCreate(ZKClient, String, AsyncFunction, Codec, List)
   * updateOrCreate(zkClient, path, modifier, codec, null)}
   *
   * @see #updateOrCreate(ZKClient, String, AsyncFunction, Codec, List)
   */
  public static <V> ListenableFuture<V> updateOrCreate(ZKClient zkClient, String path,
                                                       AsyncFunction<V, V> modifier, Codec<V> codec) {
    return updateOrCreate(zkClient, path, modifier, codec, null);
  }

  /**
   * Update the content of the given node. If the node doesn't exist, it will try to create the node. If the node
   * exists, the existing content of the data will be provided to the modifier function to generate new content. A
   * conditional set will be performed which requires existing content the same as the one provided to the modifier
   * function. If the conditional set failed, the latest content will be fetched and fed to the modifier function
   * again.
   * This will continue until the set is successful or the modifier gave up the update, by returning {@code null}.
   *
   * @param zkClient The ZKClient to perform the operations.
   * @param path The path in ZK.
   * @param modifier A function to generate new content
   * @param codec Codec to encode/decode content to/from byte array
   * @param createAcl If not {@code null}, the access control list to set on the node, if it is created.
   * @param <V> Type of the content
   * @return A {@link ListenableFuture} that will be completed when node is created or data is set.
   *         The future will carry the actual content being set into the node. The future will
   *         fail if failed to create and to set the data. Calling {@link ListenableFuture#cancel(boolean)} has
   *         no effect.
   */
  public static <V> ListenableFuture<V> updateOrCreate(ZKClient zkClient, String path,
                                                       AsyncFunction<V, V> modifier, Codec<V> codec,
                                                       @Nullable List<ACL> createAcl) {
    SettableFuture<V> resultFuture = SettableFuture.create();
    getAndSet(zkClient, path, modifier, codec, resultFuture, createAcl);
    return resultFuture;
  }

  /**
   * Performs the get and condition set part as described in
   * {@link #updateOrCreate(ZKClient, String, Function, Codec, List)}.
   */
  private static <V> void getAndSet(final ZKClient zkClient, final String path,
                                    final AsyncFunction<V, V> modifier, final Codec<V> codec,
                                    final SettableFuture<V> resultFuture, final List<ACL> createAcl) {

    // Try to fetch the node data
    Futures.addCallback(zkClient.getData(path), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(final NodeData result) {
        try {
          // Node has data. Call modifier to get newer version of content
          final int version = result.getStat().getVersion();

          Futures.addCallback(modifier.apply(codec.decode(result.getData())), new FutureCallback<V>() {
            @Override
            public void onSuccess(final V content) {
              // When modifier calls completed, try to set the content

              // Modifier decided to abort
              if (content == null) {
                resultFuture.set(null);
                return;
              }
              try {
                byte[] data = codec.encode(content);

                // No change in content. No need to update and simply set the future to complete.
                if (Arrays.equals(data, result.getData())) {
                  resultFuture.set(content);
                  return;
                }

                Futures.addCallback(zkClient.setData(path, data, version), new FutureCallback<Stat>() {
                  @Override
                  public void onSuccess(Stat result) {
                    resultFuture.set(content);
                  }

                  @Override
                  public void onFailure(Throwable t) {
                    if (t instanceof KeeperException.BadVersionException) {
                      // If the version is not good, get and set again
                      getAndSet(zkClient, path, modifier, codec, resultFuture, createAcl);
                    } else if (t instanceof KeeperException.NoNodeException) {
                      // If the node doesn't exist, try to do create
                      createOrGetAndSet(zkClient, path, modifier, codec, resultFuture, createAcl);
                    } else {
                      resultFuture.setException(t);
                    }
                  }
                }, Threads.SAME_THREAD_EXECUTOR);
              } catch (Throwable t) {
                resultFuture.setException(t);
              }
            }

            @Override
            public void onFailure(Throwable t) {
              resultFuture.setException(t);
            }
          }, Threads.SAME_THREAD_EXECUTOR);

        } catch (Throwable t) {
          resultFuture.setException(t);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // If failed to get data because node doesn't exist, try the create.
        if (t instanceof KeeperException.NoNodeException) {
          createOrGetAndSet(zkClient, path, modifier, codec, resultFuture, createAcl);
        } else {
          resultFuture.setException(t);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  /**
   * Performs the create part as described in
   * {@link #updateOrCreate(ZKClient, String, Function, Codec, List)}. If the creation failed with
   * {@link KeeperException.NodeExistsException}, the
   * {@link #getAndSet(ZKClient, String, AsyncFunction, Codec, SettableFuture, List)} will be called.
   */
  private static <V> void createOrGetAndSet(final ZKClient zkClient, final String path,
                                            final AsyncFunction<V, V> modifier, final Codec<V> codec,
                                            final SettableFuture<V> resultFuture, final List<ACL> acls) {

    // Tries to create the node first.
    ListenableFuture<V> createFuture = create(zkClient, path, new Supplier<ListenableFuture<V>>() {
      @Override
      public ListenableFuture<V> get() {
        try {
          return Futures.transform(modifier.apply(null), new Function<V, V>() {
            @Override
            public V apply(@Nullable V input) {
              // If the modifier returns null, it means aborting the operation
              // we throw an exception here so that it will be reflected in the "createFuture".
              // The callback on the createFuture will handle this exception
              if (input == null) {
                throw new AbortModificationException();
              } else {
                return input;
              }
            }
          }, Threads.SAME_THREAD_EXECUTOR);
        } catch (Exception e) {
          return Futures.immediateFailedFuture(e);
        }
      }
    }, codec, acls, SettableFuture.<V>create());

    try {
      Futures.addCallback(createFuture, new FutureCallback<V>() {
        @Override
        public void onSuccess(final V content) {
          resultFuture.set(content);
        }

        @Override
        public void onFailure(Throwable t) {
          if (t instanceof AbortModificationException) {
            // The modifier decided to abort. Just set the result future to complete
            resultFuture.set(null);
          } else if (t instanceof KeeperException.NodeExistsException) {
            // If failed to create due to node exists, try to do getAndSet.
            getAndSet(zkClient, path, modifier, codec, resultFuture, acls);
          } else {
            resultFuture.setException(t);
          }
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    } catch (Throwable e) {
      resultFuture.setException(e);
    }
  }

  /**
   * Actual implementation of the three public methods.
   *
   * @see #createOrSet(ZKClient, String, Supplier, Codec, int)
   * @see #createOrSet(ZKClient, String, Supplier, Codec, int, List)
   * @see #setOrCreate(ZKClient, String, Supplier, Codec, int)
   */
  private static <T> ListenableFuture<T> createOrSetWithRetry(final boolean createFirst, final ZKClient zkClient,
                                                              final String path, final Supplier<T> dataSupplier,
                                                              final Codec<T> codec, @Nullable final Iterable<ACL> acls,
                                                              final int maxRetry) {
    final SettableFuture<T> resultFuture = SettableFuture.create();
    final AtomicInteger failures = new AtomicInteger(0);

    Futures.addCallback(
      doCreateOrSet(createFirst, zkClient, path, dataSupplier, codec, acls),
      new FutureCallback<T>() {
        @Override
        public void onSuccess(T result) {
          resultFuture.set(result);
        }

        @Override
        public void onFailure(Throwable t) {
          if (failures.getAndIncrement() < maxRetry) {
            Futures.addCallback(doCreateOrSet(createFirst, zkClient, path, dataSupplier, codec, acls),
                                this, Threads.SAME_THREAD_EXECUTOR);
          } else {
            resultFuture.setException(t);
          }
        }
      }, Threads.SAME_THREAD_EXECUTOR
    );

    return resultFuture;
  }

  /**
   * Tries create or set the content of a node. If {@code createFirst} is {@code true}, it will first try to create;
   * and if it failed with NodeExists error, then will try to perform set.
   * If {@code createFirst} is {@code false}, then it will first try to set and if it failed with NoNode error,
   * then will try to perform create.
   *
   * @param createFirst if {@code true}, try create and then set; if {@code false}, try set and then create.
   * @param zkClient The ZKClient to perform the operations.
   * @param path The path in ZK.
   * @param dataSupplier The supplier to provide the content to be set to the node.
   * @param codec A {@link Codec} for serializing the data into byte array.
   * @param acls The access control list to set on the node.
   * @param <T> type of the data to set to the node
   * @return A {@link ListenableFuture} that will be completed when node is created or data is set.
   *         The future will carry the actual content being set into the node.
   */
  private static <T> ListenableFuture<T> doCreateOrSet(final boolean createFirst, final ZKClient zkClient,
                                                       final String path, final Supplier<T> dataSupplier,
                                                       final Codec<T> codec, @Nullable final Iterable<ACL> acls) {
    final SettableFuture<T> resultFuture = SettableFuture.create();
    final Supplier<ListenableFuture<T>> futureSupplier = createFutureSupplier(dataSupplier);
    try {
      // Do a create/set first based on the argument
      ListenableFuture<T> future = createFirst
        ? create(zkClient, path, futureSupplier, codec, acls, SettableFuture.<T>create())
        : setData(zkClient, path, dataSupplier, codec, SettableFuture.<T>create());

      Futures.addCallback(future, new FutureCallback<T>() {
        @Override
        public void onSuccess(T result) {
          // If the operation completed, then we are done
          resultFuture.set(result);
        }

        @Override
        public void onFailure(Throwable failure) {
          if (createFirst && failure instanceof KeeperException.NodeExistsException) {
            // If failed to create due to node exists exception, set the value
            setData(zkClient, path, dataSupplier, codec, resultFuture);
          } else if (!createFirst && failure instanceof KeeperException.NoNodeException) {
            // If failed to set due to no node exception, try to create the node
            create(zkClient, path, futureSupplier, codec, acls, resultFuture);
          } else {
            resultFuture.setException(failure);
          }
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    } catch (Exception e) {
      resultFuture.setException(e);
    }
    return resultFuture;
  }

  /**
   * Creates a zookeeper node with the given data with automatic parent node creation.
   *
   * @param zkClient The ZKClient to perform the operations.
   * @param path The path in ZK.
   * @param dataSupplier a {@link Supplier} to provide a {@link ListenableFuture} that will yield data to be used
   *                     as the node content. The supplier may get invoked
   *                     multiple times when the actual data is needed for creating the content of
   *                     the given node. The supplier can be invoked from the caller thread as well as the
   *                     zookeeper event callback thread.
   * @param codec A {@link Codec} for serializing the data into byte array.
   * @param acls The access control list to set on the node.
   * @param resultFuture a {@link SettableFuture} for reflecting the operation result. If the create succeeded,
   *                     the result future will contain the actual object being set to the node.
   * @param <T> type of data content
   * @return the same resultFuture as being passed from parameter
   */
  private static <T> SettableFuture<T> create(final ZKClient zkClient, final String path,
                                              final Supplier<ListenableFuture<T>> dataSupplier, final Codec<T> codec,
                                              @Nullable final Iterable<ACL> acls,
                                              final SettableFuture<T> resultFuture) {
    // Invoke the supplier to get a ListenableFuture and performs node create when the data is available
    Futures.addCallback(dataSupplier.get(), new FutureCallback<T>() {
      @Override
      public void onSuccess(final T data) {
        try {
          // Try to create the node without creating parent. This is to make sure the latest data
          // is being used if there are concurrent modification to the node (CDAP-4388)
          OperationFuture<String> createFuture = (acls == null)
            ? zkClient.create(path, codec.encode(data), CreateMode.PERSISTENT, false)
            : zkClient.create(path, codec.encode(data), CreateMode.PERSISTENT, false, acls);

          Futures.addCallback(createFuture, new FutureCallback<String>() {
            @Override
            public void onSuccess(String result) {
              // If creation succeeded, the operation is completed
              resultFuture.set(data);
            }

            @Override
            public void onFailure(Throwable t) {
              if (t instanceof KeeperException.NoNodeException) {
                // If failed with NoNode, it means parent path doesn't exist. Create the parent path first and retry
                OperationFuture<String> createParentFuture = zkClient.create(getParent(path), null,
                                                                             CreateMode.PERSISTENT);
                Futures.addCallback(createParentFuture, new FutureCallback<String>() {
                  @Override
                  public void onSuccess(String result) {
                    // If creation of parent path succeeded, try to create again.
                    // We call the create method again so that the dataSupplier will be called to get the latest value.
                    create(zkClient, path, dataSupplier, codec, acls, resultFuture);
                  }

                  @Override
                  public void onFailure(Throwable t) {
                    // If failed to create parent path, fail the operation
                    resultFuture.setException(t);
                  }
                }, Threads.SAME_THREAD_EXECUTOR);
              } else {
                resultFuture.setException(t);
              }
            }
          }, Threads.SAME_THREAD_EXECUTOR);
        } catch (Exception e) {
          resultFuture.setException(e);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // If the supplier failed to give the node content, reflect the error in the result future.
        resultFuture.setException(t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    return resultFuture;
  }

  /**
   * Sets the data content of a zookeeper node
   *
   * @param zkClient The ZKClient to perform the operations.
   * @param path The path in ZK.
   * @param dataSupplier The supplier to provide the content to be set to the node.
   * @param codec A {@link Codec} for serializing the data into byte array.
   * @param resultFuture a {@link SettableFuture} for reflecting the operation result. If the setData succeeded,
   *                     the result future will contain the actual object being set to the node.
   * @param <T> type of data content
   * @return the same resultFuture as being passed from parameter
   */
  private static <T> SettableFuture<T> setData(ZKClient zkClient, String path,
                                               Supplier<T> dataSupplier, Codec<T> codec,
                                               final SettableFuture<T> resultFuture) {
    try {
      final T data = dataSupplier.get();
      Futures.addCallback(zkClient.setData(path, codec.encode(data)), new FutureCallback<Stat>() {
        @Override
        public void onSuccess(Stat state) {
          resultFuture.set(data);
        }

        @Override
        public void onFailure(Throwable t) {
          resultFuture.setException(t);
        }
      }, Threads.SAME_THREAD_EXECUTOR);
    } catch (Exception e) {
      resultFuture.setException(e);
    }

    return resultFuture;
  }

  /**
   * Creates a {@link Supplier} such that when invoked, it will invoke the given {@link Supplier} and wrap
   * the result with a {@link ListenableFuture}.
   */
  private static <T> Supplier<ListenableFuture<T>> createFutureSupplier(final Supplier<T> supplier) {
    return new Supplier<ListenableFuture<T>>() {
      @Override
      public ListenableFuture<T> get() {
        return Futures.immediateFuture(supplier.get());
      }
    };
  }

  /**
   * Gets the parent of the given path.
   * @param path Path for computing its parent
   * @return Parent of the given path, or empty string if the given path is the root path already.
   */
  private static String getParent(String path) {
    String parentPath = path.substring(0, path.lastIndexOf('/'));
    return (parentPath.isEmpty() && !"/".equals(path)) ? "/" : parentPath;
  }

  /**
   * An exception to indicate that the modification operation is aborted. This exception won't get propagated to
   * user, but instead used as a tagging exception for aborting the createOrGetAndSet operation.
   */
  private static final class AbortModificationException extends RuntimeException {
    // No-op
  }


  private ZKExtOperations() {
  }
}
