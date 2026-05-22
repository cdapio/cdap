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
package org.apache.twill.internal.zookeeper;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.ACLData;
import org.apache.twill.zookeeper.ForwardingZKClient;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.RetryStrategy;
import org.apache.twill.zookeeper.RetryStrategy.OperationType;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * A {@link ZKClient} that will invoke {@link RetryStrategy} on operation failure.
 * This {@link ZKClient} works by delegating calls to another {@link ZKClient}
 * and listen for the result. If the result is a failure, and is
 * {@link RetryUtils#canRetry(org.apache.zookeeper.KeeperException.Code) retryable}, the given {@link RetryStrategy}
 * will be called to determine the next retry time, or give up, depending on the value returned by the strategy.
 */
public final class FailureRetryZKClient extends ForwardingZKClient {

  private static final ScheduledExecutorService SCHEDULER = Executors.newSingleThreadScheduledExecutor(
                                                                Threads.createDaemonThreadFactory("retry-zkclient"));
  private final RetryStrategy retryStrategy;

  public FailureRetryZKClient(ZKClient delegate, RetryStrategy retryStrategy) {
    super(delegate);
    this.retryStrategy = retryStrategy;
  }

  @Override
  public OperationFuture<String> create(final String path, @Nullable final byte[] data, final CreateMode createMode,
                                        final boolean createParent, final Iterable<ACL> acl) {
    // No retry for any SEQUENTIAL node, as some algorithms depends on only one sequential node being created.
    if (createMode == CreateMode.PERSISTENT_SEQUENTIAL || createMode == CreateMode.EPHEMERAL_SEQUENTIAL) {
      return super.create(path, data, createMode, createParent, acl);
    }

    final SettableOperationFuture<String> result = SettableOperationFuture.create(path, Threads.SAME_THREAD_EXECUTOR);
    Futures.addCallback(super.create(path, data, createMode, createParent, acl),
                        new OperationFutureCallback<String>(OperationType.CREATE, System.currentTimeMillis(),
                                                            path, result, new Supplier<OperationFuture<String>>() {
                          @Override
                          public OperationFuture<String> get() {
                            return FailureRetryZKClient.super.create(path, data, createMode, createParent, acl);
                          }
                        }), MoreExecutors.directExecutor());
    return result;
  }

  @Override
  public OperationFuture<Stat> exists(final String path, final Watcher watcher) {
    final SettableOperationFuture<Stat> result = SettableOperationFuture.create(path, Threads.SAME_THREAD_EXECUTOR);
    Futures.addCallback(super.exists(path, watcher),
                        new OperationFutureCallback<Stat>(OperationType.EXISTS, System.currentTimeMillis(),
                                                          path, result, new Supplier<OperationFuture<Stat>>() {
                          @Override
                          public OperationFuture<Stat> get() {
                            return FailureRetryZKClient.super.exists(path, watcher);
                          }
                        }), MoreExecutors.directExecutor());
    return result;
  }

  @Override
  public OperationFuture<NodeChildren> getChildren(final String path, final Watcher watcher) {
    final SettableOperationFuture<NodeChildren> result = SettableOperationFuture.create(path,
                                                                                        Threads.SAME_THREAD_EXECUTOR);
    Futures.addCallback(super.getChildren(path, watcher),
                        new OperationFutureCallback<NodeChildren>(OperationType.GET_CHILDREN,
                                                                  System.currentTimeMillis(), path, result,
                                                                  new Supplier<OperationFuture<NodeChildren>>() {
                          @Override
                          public OperationFuture<NodeChildren> get() {
                            return FailureRetryZKClient.super.getChildren(path, watcher);
                          }
                        }), MoreExecutors.directExecutor());
    return result;
  }

  @Override
  public OperationFuture<NodeData> getData(final String path, final Watcher watcher) {
    final SettableOperationFuture<NodeData> result = SettableOperationFuture.create(path, Threads.SAME_THREAD_EXECUTOR);
    Futures.addCallback(super.getData(path, watcher),
                        new OperationFutureCallback<NodeData>(OperationType.GET_DATA, System.currentTimeMillis(),
                                                              path, result, new Supplier<OperationFuture<NodeData>>() {
                          @Override
                          public OperationFuture<NodeData> get() {
                            return FailureRetryZKClient.super.getData(path, watcher);
                          }
                        }), MoreExecutors.directExecutor());
    return result;
  }

  @Override
  public OperationFuture<Stat> setData(final String dataPath, final byte[] data, final int version) {
    final SettableOperationFuture<Stat> result = SettableOperationFuture.create(dataPath, Threads.SAME_THREAD_EXECUTOR);
    Futures.addCallback(super.setData(dataPath, data, version),
                        new OperationFutureCallback<Stat>(OperationType.SET_DATA, System.currentTimeMillis(),
                                                          dataPath, result, new Supplier<OperationFuture<Stat>>() {
                          @Override
                          public OperationFuture<Stat> get() {
                            return FailureRetryZKClient.super.setData(dataPath, data, version);
                          }
                        }), MoreExecutors.directExecutor());
    return result;
  }

  @Override
  public OperationFuture<String> delete(final String deletePath, final int version) {
    final SettableOperationFuture<String> result = SettableOperationFuture.create(deletePath,
                                                                                  Threads.SAME_THREAD_EXECUTOR);
    Futures.addCallback(super.delete(deletePath, version),
                        new OperationFutureCallback<String>(OperationType.DELETE, System.currentTimeMillis(),
                                                            deletePath, result, new Supplier<OperationFuture<String>>
                          () {
                          @Override
                          public OperationFuture<String> get() {
                            return FailureRetryZKClient.super.delete(deletePath, version);
                          }
                        }), MoreExecutors.directExecutor());
    return result;
  }

  @Override
  public OperationFuture<ACLData> getACL(final String path) {
    final SettableOperationFuture<ACLData> result = SettableOperationFuture.create(path, Threads.SAME_THREAD_EXECUTOR);
    Futures.addCallback(super.getACL(path),
                        new OperationFutureCallback<ACLData>(OperationType.GET_ACL, System.currentTimeMillis(),
                                                          path, result, new Supplier<OperationFuture<ACLData>>() {
                          @Override
                          public OperationFuture<ACLData> get() {
                            return FailureRetryZKClient.super.getACL(path);
                          }
                        }), MoreExecutors.directExecutor());
    return result;
  }

  @Override
  public OperationFuture<Stat> setACL(final String path, final Iterable<ACL> acl, final int version) {
    final SettableOperationFuture<Stat> result = SettableOperationFuture.create(path, Threads.SAME_THREAD_EXECUTOR);
    Futures.addCallback(super.setACL(path, acl, version),
                        new OperationFutureCallback<Stat>(OperationType.SET_ACL, System.currentTimeMillis(),
                                                          path, result, new Supplier<OperationFuture<Stat>>() {
                          @Override
                          public OperationFuture<Stat> get() {
                            return FailureRetryZKClient.super.setACL(path, acl, version);
                          }
                        }), MoreExecutors.directExecutor());
    return result;
  }

  /**
   * Callback to watch for operation result and trigger retry if necessary.
   * @param <V> Type of operation result.
   */
  private final class OperationFutureCallback<V> implements FutureCallback<V> {

    private final OperationType type;
    private final long startTime;
    private final String path;
    private final SettableOperationFuture<V> result;
    private final Supplier<OperationFuture<V>> retryAction;
    private final AtomicInteger failureCount;

    private OperationFutureCallback(OperationType type, long startTime, String path,
                                    SettableOperationFuture<V> result, Supplier<OperationFuture<V>> retryAction) {
      this.type = type;
      this.startTime = startTime;
      this.path = path;
      this.result = result;
      this.retryAction = retryAction;
      this.failureCount = new AtomicInteger(0);
    }

    @Override
    public void onSuccess(V result) {
      this.result.set(result);
    }

    @Override
    public void onFailure(Throwable t) {
      if (!doRetry(t)) {
        result.setException(t);
      }
    }

    private boolean doRetry(Throwable t) {
      if (!RetryUtils.canRetry(t)) {
        return false;
      }

      // Determine the relay delay
      long nextRetry = retryStrategy.nextRetry(failureCount.incrementAndGet(), startTime, type, path);
      if (nextRetry < 0) {
        return false;
      }

      // Schedule the retry.
      SCHEDULER.schedule(new Runnable() {
        @Override
        public void run() {
          Futures.addCallback(retryAction.get(), OperationFutureCallback.this, MoreExecutors.directExecutor());
        }
      }, nextRetry, TimeUnit.MILLISECONDS);

      return true;
    }
  }
}
