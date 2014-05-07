/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

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

  private ZKExtOperations() {
  }
}
