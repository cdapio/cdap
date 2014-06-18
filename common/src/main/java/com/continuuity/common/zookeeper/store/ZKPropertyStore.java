/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.common.zookeeper.store;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.AbstractPropertyStore;
import com.continuuity.common.conf.PropertyUpdater;
import com.continuuity.common.io.Codec;
import com.continuuity.common.zookeeper.ZKExtOperations;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * This class uses ZK for storing properties/configures. It provides update methods for updating properties,
 * and listener methods for watching for changes in properties.
 *
 * TODO: Unify this and SharedResourceCache in security module.
 *
 * @param <T> Type of property object
 */
public final class ZKPropertyStore<T> extends AbstractPropertyStore<T> {

  private static final Logger LOG = LoggerFactory.getLogger(ZKPropertyStore.class);
  private static final int MAX_ZK_FAILURE_RETRIES = 10;

  private final ZKClient zkClient;
  private final Codec<T> codec;
  private final Set<String> watchedSet;

  /**
   * Creates an instance of {@link ZKPropertyStore}.
   *
   * @param zkClient client for interacting with ZooKeeper. Nodes will be created at root represented by this ZKClient.
   * @param codec The codec for encode/decode property
   */
  public static <T> ZKPropertyStore<T> create(ZKClient zkClient, Codec<T> codec) {
    return new ZKPropertyStore<T>(zkClient, codec);
  }

  /**
   * Creates an instance of {@link ZKPropertyStore} with nodes created under the given namespace.
   *
   * @param zkClient client for interacting with ZooKeeper
   * @param namespace Namespace for zk nodes to reside in
   * @param codec The codec for encode/decode property
   */
  public static <T> ZKPropertyStore<T> create(ZKClient zkClient, String namespace, Codec<T> codec) {
    return new ZKPropertyStore<T>(ZKClients.namespace(zkClient, namespace), codec);
  }

  /**
   * Constructor.
   */
  private ZKPropertyStore(ZKClient zkClient, Codec<T> codec) {
    this.zkClient = zkClient;
    this.codec = codec;
    this.watchedSet = Sets.newHashSet();
  }

  @Override
  public ListenableFuture<T> update(String name, PropertyUpdater<T> updater) {
    return ZKExtOperations.updateOrCreate(zkClient, getPath(name), updater, codec);
  }

  @Override
  public ListenableFuture<T> set(String name, T property) {
    try {
      return ZKExtOperations.setOrCreate(zkClient, getPath(name),
                                         codec.encode(property), property, MAX_ZK_FAILURE_RETRIES);
    } catch (IOException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  @Override
  protected synchronized boolean listenerAdded(String name) {
    if (watchedSet.add(name)) {
      // Start watching for node change and maintain cached value.
      // Invocation of listener would be triggered inside ZK callback.
      existsAndWatch(name);
      return false;
    }

    // Invoke it with the cached property if available when first added.
    // If no cache value exists, meaning either property was removed or still pending for update for the first time
    // For first case, no need to invoke listener. For second case, when the cache get updated, the newly
    // added listener would get triggered
    return true;
  }

  private String getPath(String name) {
    return "/" + name;
  }

  private void getDataAndWatch(final String name) {
    Futures.addCallback(zkClient.getData(getPath(name), new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (isClosed()) {
          return;
        }

        if (event.getType() == Event.EventType.NodeDeleted) {
          existsAndWatch(name);
        } else {
          getDataAndWatch(name);
        }
      }
    }), new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        byte[] data = result.getData();
        if (data == null) {
          updateAndNotify(name, null);
        } else {
          try {
            updateAndNotify(name, codec.decode(data));
          } catch (IOException e) {
            LOG.error("Failed to decode property data for {}: {}", name, Bytes.toStringBinary(data), e);
            notifyError(name, e);
          }
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (t instanceof KeeperException.NoNodeException) {
          // If node not exists, watch for exists.
          existsAndWatch(name);
        } else {
          LOG.error("Failed to get property data for {}", name, t);
          notifyError(name, t);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private void existsAndWatch(final String name) {
    Futures.addCallback(zkClient.exists(getPath(name), new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        if (isClosed()) {
          return;
        }

        // If the event is not node created, meaning the node was existed.
        // Hence getDataAndWatch should be handling that case already
        if (event.getType() == Event.EventType.NodeCreated) {
          getDataAndWatch(name);
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(Stat result) {
        // If the node exists, call getData. Otherwise, the watcher should handle the case when the node is created
        if (result != null) {
          getDataAndWatch(name);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to check exists for property data for {}", name, t);
        notifyError(name, t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }
}
