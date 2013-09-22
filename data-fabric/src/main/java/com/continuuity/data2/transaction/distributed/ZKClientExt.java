package com.continuuity.data2.transaction.distributed;

import com.continuuity.weave.zookeeper.NodeChildren;
import com.continuuity.weave.zookeeper.NodeData;
import com.continuuity.weave.zookeeper.OperationFuture;
import com.continuuity.weave.zookeeper.ZKClient;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import javax.annotation.Nullable;

/**
 * Extension to standard operations provided by {@link com.continuuity.weave.zookeeper.ZKClient}
 */
// todo: remove it in favor of generic leader election tool
public final class ZKClientExt {
  private ZKClientExt() {}

  /**
   * Same as calling {@link #createOrSet(com.continuuity.weave.zookeeper.ZKClient, String, byte[], org.apache.zookeeper.CreateMode, boolean)
   * createOrSet(zkClient, path, data, createMode, true)}
   */
  public static ListenableFuture<SetResult> createOrSet(final ZKClient zkClient,
                                                     final String path, @Nullable final byte[] data,
                                                     final CreateMode createMode) {
    return createOrSet(zkClient, path, data, createMode, true);
  }

  /**
   * Acts as {@link com.continuuity.weave.zookeeper.ZKClient#create(String, byte[], org.apache.zookeeper.CreateMode, boolean)} if node does not exist,
   * otherwise as {@link com.continuuity.weave.zookeeper.ZKClient#setData(String, byte[])}.
   */
  public static ListenableFuture<SetResult> createOrSet(final ZKClient zkClient, final String path,
                                                     @Nullable final byte[] data, final CreateMode createMode,
                                                     final boolean createParent) {
    final SettableFuture<SetResult> resultFuture = SettableFuture.create();

    final OperationFuture<String> createResult = zkClient.create(path, data, createMode, createParent);
    Futures.addCallback(createResult, new FutureCallback<String>() {
      private final FutureCallback<String> createCallback = this;

      @Override
      public void onSuccess(String result) {
        resultFuture.set(new SetResult(result, null));
      }

      @Override
      public void onFailure(Throwable t) {
        if (causedBy(t, KeeperException.NodeExistsException.class)) {
          OperationFuture<Stat> setDataResult = zkClient.setData(path, data);
          Futures.addCallback(setDataResult, new FutureCallback<Stat>() {
            @Override
            public void onSuccess(Stat result) {
              resultFuture.set(new SetResult(null, result));
            }

            @Override
            public void onFailure(Throwable t) {
              if (causedBy(t, KeeperException.NoNodeException.class)) {
                Futures.addCallback(zkClient.create(path, data, createMode, createParent), createCallback);
                return;
              }
              resultFuture.setException(t);
            }
          });
          return;
        }
        resultFuture.setException(t);
      }
    });

    return resultFuture;
  }

  /**
   * Result of {@link #createOrSet(com.continuuity.weave.zookeeper.ZKClient, String, byte[], org.apache.zookeeper.CreateMode, boolean)} operation.
   * {@link #getPath()}
   * {@link #getPath()} returns null if no new node was created, otherwise the new node path.
   */
  public static final class SetResult {
    private String path;
    private Stat stat;

    public SetResult(String path, Stat stat) {
      this.path = path;
      this.stat = stat;
    }

    /**
     * Returns null if no new node was created, otherwise the new node path.
     */
    public String getPath() {
      return path;
    }

    /**
     * Returns null if new node was created, otherwise the result of the set operation.
     */
    public Stat getStat() {
      return stat;
    }
  }

  /**
   * Same as calling {@link #delete(com.continuuity.weave.zookeeper.ZKClient, String, int, boolean)} with {@code version=-1}
   */
  public static ListenableFuture<String> delete(final ZKClient zkClient, final String path, boolean ignoreIfAbsent) {
    return delete(zkClient, path, -1, ignoreIfAbsent);
  }

  /**
   * Acts as {@link com.continuuity.weave.zookeeper.ZKClient#delete(String)} if passed {@code ignoreIfAbsent} param is false.
   * Otherwise the same way but doesn't throw exception if node doesn't exists.
   * In latter case sets {@code null} in returned future.
   */
  public static ListenableFuture<String> delete(final ZKClient zkClient,
                                                final String path,
                                                final int version,
                                                boolean ignoreIfAbsent) {
    if (!ignoreIfAbsent) {
      return zkClient.delete(path, version);
    }
    return ignoreError(zkClient.delete(path, version), KeeperException.NoNodeException.class);
  }

  /**
   * Acts as {@link com.continuuity.weave.zookeeper.ZKClient#create(String, byte[], org.apache.zookeeper.CreateMode, boolean)
   * create(path, null, CreateMode.PERSISTENT, true)} if node doesn't exist. Otherwise has no affect.
   * In latter case sets {@code null} in returned future.
   */
  public static ListenableFuture<String> ensureExists(final ZKClient zkClient,
                                                      final String path) {
    final SettableFuture<String> resultFuture = SettableFuture.create();
    OperationFuture<String> createFuture = zkClient.create(path, null, CreateMode.PERSISTENT, true);
    Futures.addCallback(createFuture, new FutureCallback<String>() {
      @Override
      public void onSuccess(String result) {
        resultFuture.set(result);
      }

      @Override
      public void onFailure(Throwable t) {
        if (causedBy(t, KeeperException.NodeExistsException.class)) {
          resultFuture.set(path);
        } else {
          resultFuture.setException(t);
        }
      }
    });

    return resultFuture;
  }

  /**
   * Same as calling {@link #getDataOrNull(com.continuuity.weave.zookeeper.ZKClient, String, org.apache.zookeeper.Watcher)} with {@code watcher=null}
   */
  public static ListenableFuture<NodeData> getDataOrNull(final ZKClient zkClient, final String path) {
    return getDataOrNull(zkClient, path, null);
  }

  /**
   * Acts as {@link com.continuuity.weave.zookeeper.ZKClient#getData(String, org.apache.zookeeper.Watcher)} if node exists.
   * Otherwise sets {@code null} in returned future.
   */
  public static ListenableFuture<NodeData> getDataOrNull(final ZKClient zkClient,
                                                         final String path,
                                                         @Nullable Watcher watcher) {
    return ignoreError(zkClient.getData(path, watcher), KeeperException.NoNodeException.class);
  }

  /**
   * Same as calling {@link #getChildrenOrNull(com.continuuity.weave.zookeeper.ZKClient, String, org.apache.zookeeper.Watcher)}
   * with {@code watcher=null}
   */
  public static ListenableFuture<NodeChildren> getChildrenOrNull(final ZKClient zkClient,
                                                   final String path) {
    return getChildrenOrNull(zkClient, path, null);
  }

  /**
   * Acts as {@link com.continuuity.weave.zookeeper.ZKClient#getChildren(String, org.apache.zookeeper.Watcher)} if node exists.
   * Otherwise sets {@code null} in returned future.
   */
  public static ListenableFuture<NodeChildren> getChildrenOrNull(final ZKClient zkClient,
                                                   final String path,
                                                   @Nullable Watcher watcher) {

    return ignoreError(zkClient.getChildren(path, watcher), KeeperException.NoNodeException.class);
  }

  private static <T>ListenableFuture<T> ignoreError(final ListenableFuture<T> future,
                                                    final Class<? extends KeeperException> ex) {

    final SettableFuture<T> futureWithIgnoredError = SettableFuture.create();
    Futures.addCallback(future, new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        futureWithIgnoredError.set(result);
      }

      @Override
      public void onFailure(Throwable t) {
        if (causedBy(t, ex)) {
          futureWithIgnoredError.set(null);
        } else {
          futureWithIgnoredError.setException(t);
        }
      }
    });

    return futureWithIgnoredError;
  }

  private static boolean causedBy(Throwable t, Class<? extends KeeperException> ex) {
    return ex.isAssignableFrom(t.getClass()) || (t.getCause() != null && ex.isAssignableFrom(t.getCause().getClass()));
  }
}
