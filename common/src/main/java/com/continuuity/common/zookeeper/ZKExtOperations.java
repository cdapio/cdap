/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper;

import com.continuuity.common.async.AsyncFunctions;
import com.continuuity.common.io.Codec;
import com.google.common.base.Function;
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
   * @param data The content of the ZK node.
   * @param result The result that will be set into the result future when completed successfully.
   * @param maxFailure Maximum number of times to try to create/set the content.
   * @param <V> Type of the result.
   * @return A {@link ListenableFuture} that will be completed when node is created or data is set. The future will
   *         fail if failed to create and to set the data. Calling {@link ListenableFuture#cancel(boolean)} has
   *         no effect.
   */
  public static <V> ListenableFuture<V> createOrSet(ZKClient zkClient, String path,
                                                    byte[] data, V result, int maxFailure) {
    return setContent(zkClient, path, data, result, maxFailure, true, null);
  }

  /**
   * Attempts to create a persistent node with the given content. If creation failed because the node already
   * exists ({@link KeeperException.NodeExistsException}), the node will be set with the given content.
   * This method is suitable for cases where the node expected to be non-existed.
   *
   * @param zkClient The ZKClient to perform the operations.
   * @param path The path in ZK.
   * @param data The content of the ZK node.
   * @param result The result that will be set into the result future when completed successfully.
   * @param maxFailure Maximum number of times to try to create/set the content.
   * @param createAcl The access control list to set on the node, if it is created.
   * @param <V> Type of the result.
   * @return A {@link ListenableFuture} that will be completed when node is created or data is set. The future will
   *         fail if failed to create and to set the data. Calling {@link ListenableFuture#cancel(boolean)} has
   *         no effect.
   */
  public static <V> ListenableFuture<V> createOrSet(ZKClient zkClient, String path,
                                                    byte[] data, V result, int maxFailure, List<ACL> createAcl) {
    return setContent(zkClient, path, data, result, maxFailure, true, createAcl);
  }

  /**
   * Attempts to set the content of the given node. If it failed due to node not exists
   * ({@link KeeperException.NoNodeException}), a persistent node will be created with the given content.
   * This method is suitable for cases where the node is expected to be existed.
   *
   * @param zkClient The ZKClient to perform the operations.
   * @param path The path in ZK.
   * @param data The content of the ZK node.
   * @param result The result that will be set into the result future when completed successfully.
   * @param maxFailure Maximum number of times to try to create/set the content.
   * @param <V> Type of the result.
   * @return A {@link ListenableFuture} that will be completed when node is created or data is set. The future will
   *         fail if failed to create and to set the data. Calling {@link ListenableFuture#cancel(boolean)} has
   *         no effect.
   */
  public static <V> ListenableFuture<V> setOrCreate(ZKClient zkClient, String path,
                                                    byte[] data, V result, int maxFailure) {
    return setContent(zkClient, path, data, result, maxFailure, false, null);
  }

  /**
   * Update the content of the given node. If the node doesn't exists, it will try to create the node. Same as calling
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
   * Update the content of the given node. If the node doesn't exists, it will try to create the node.
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
   * Update the content of the given node. If the node doesn't exists, it will try to create the node. Same as calling
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
   * Update the content of the given node. If the node doesn't exists, it will try to create the node. If the node
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
   * Attempts to set the content of the given node. If it failed due to node not exists
   * ({@link KeeperException.NoNodeException}), a persistent node will be created with the given content.
   * This method is suitable for cases where the node is expected to be existed.
   *
   * @param zkClient The ZKClient to perform the operations.
   * @param path The path in ZK.
   * @param data The content of the ZK node.
   * @param result The result that will be set into the result future when completed successfully.
   * @param maxFailure Maximum number of times to try to create/set the content.
   * @param createAcl The access control list to set on the node, if it is created.
   * @param <V> Type of the result.
   * @return A {@link ListenableFuture} that will be completed when node is created or data is set. The future will
   *         fail if failed to create and to set the data. Calling {@link ListenableFuture#cancel(boolean)} has
   *         no effect.
   */
  public static <V> ListenableFuture<V> setOrCreate(ZKClient zkClient, String path,
                                                    byte[] data, V result, int maxFailure, List<ACL> createAcl) {
    return setContent(zkClient, path, data, result, maxFailure, false, createAcl);
  }

  /**
   * Sets the content of a ZK node. Depends on the {@code createFirst} value,
   * either {@link ZKClient#create(String, byte[], org.apache.zookeeper.CreateMode)} or
   * {@link ZKClient#setData(String, byte[])} wil be called first.
   *
   * @param zkClient The ZKClient to perform the operations.
   * @param path The path in ZK.
   * @param data The content of the ZK node.
   * @param result The result that will be set into the result future when completed successfully.
   * @param maxFailure Maximum number of times to try to create/set the content.
   * @param createFirst If true, create is called first, otherwise setData is called first.
   * @param <V> Type of the result.
   * @return A {@link ListenableFuture} that will be completed when node is created or data is set. The future will
   *         fail if failed to create and to set the data. Calling {@link ListenableFuture#cancel(boolean)} has
   *         no effect.
   */
  private static <V> ListenableFuture<V> setContent(final ZKClient zkClient, final String path,
                                                    final byte[] data, final V result,
                                                    final int maxFailure, boolean createFirst,
                                                    final List<ACL> createAcls) {

    final SettableFuture<V> resultFuture = SettableFuture.create();
    final AtomicInteger failureCount = new AtomicInteger(0);

    OperationFuture<?> operationFuture;

    if (createFirst) {
      if (createAcls != null) {
        operationFuture = zkClient.create(path, data, CreateMode.PERSISTENT, createAcls);
      } else {
        operationFuture = zkClient.create(path, data, CreateMode.PERSISTENT);
      }
    } else {
      operationFuture = zkClient.setData(path, data);
    }

    Futures.addCallback(operationFuture, new FutureCallback<Object>() {
      @Override
      public void onSuccess(Object zkResult) {
        resultFuture.set(result);
      }

      @Override
      public void onFailure(Throwable t) {
        if (failureCount.getAndIncrement() > maxFailure) {
          resultFuture.setException(new Exception("Failed more than " + maxFailure + "times", t));
        } else if (t instanceof KeeperException.NoNodeException) {
          // If node not exists, create it with the data
          OperationFuture<?> createFuture;
          if (createAcls != null) {
            createFuture = zkClient.create(path, data, CreateMode.PERSISTENT, createAcls);
          } else {
            createFuture = zkClient.create(path, data, CreateMode.PERSISTENT);
          }
          Futures.addCallback(createFuture, this, Threads.SAME_THREAD_EXECUTOR);
        } else if (t instanceof KeeperException.NodeExistsException) {
          // If the node exists when trying to create, set data.
          Futures.addCallback(zkClient.setData(path, data), this, Threads.SAME_THREAD_EXECUTOR);
        } else {
          resultFuture.setException(t);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

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
                      // If the node not exists, try to do create
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
        // If failed to get data because node not exists, try the create.
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
                                            final SettableFuture<V> resultFuture, final List<ACL> createAcl) {
    try {
      Futures.addCallback(modifier.apply(null), new FutureCallback<V>() {
        @Override
        public void onSuccess(final V content) {
          if (content == null) {
            resultFuture.set(null);
            return;
          }

          try {
            byte[] data = codec.encode(content);

            OperationFuture<String> future;
            if (createAcl == null) {
              future = zkClient.create(path, data, CreateMode.PERSISTENT);
            } else {
              future = zkClient.create(path, data, CreateMode.PERSISTENT, createAcl);
            }

            Futures.addCallback(future, new FutureCallback<String>() {
              @Override
              public void onSuccess(String result) {
                resultFuture.set(content);
              }

              @Override
              public void onFailure(Throwable t) {
                if (t instanceof KeeperException.NodeExistsException) {
                  // If failed to create due to node exists, try to do getAndSet.
                  getAndSet(zkClient, path, modifier, codec, resultFuture, createAcl);
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
    } catch (Throwable e) {
      resultFuture.setException(e);
    }
  }

  private ZKExtOperations() {
  }
}
