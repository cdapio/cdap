package com.continuuity.security.zookeeper;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.zookeeper.ZKExtOperations;
import com.continuuity.security.io.Codec;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.AbstractLoadingCache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ZooKeeper recipe to propagate changes to a shared cache across a number of listeners.  The cache entries
 * are materialized as child znodes under a common parent.
 * @param <T> The type of resource that is distributed to all participants in the cache.
 */
public class SharedResourceCache<T> extends AbstractLoadingCache<String, T> {
  private static final String ZNODE_PATH_SEP = "/";
  private static final int MAX_RETRIES = 3;
  private static final Logger LOG = LoggerFactory.getLogger(SharedResourceCache.class);

  private final List<ACL> znodeACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  //private final List<ACL> znodeACL = ZooDefs.Ids.CREATOR_ALL_ACL;
  private final ZKClient zookeeper;
  private final Codec<T> codec;
  private final String parentZnode;
  private final List<ResourceListener<T>> listeners = Lists.newArrayList();
  private ZKWatcher watcher;
  private Map<String, T> resources;

  public SharedResourceCache(ZKClient zookeeper, Codec<T> codec, String parentZnode) {
    this.zookeeper = zookeeper;
    this.codec = codec;
    this.parentZnode = parentZnode;
  }

  public void init() throws InterruptedException {
    this.watcher = new ZKWatcher();
    try {
      LOG.info("Checking for parent znode {}", parentZnode);
      if (zookeeper.exists(parentZnode).get() == null) {
        zookeeper.create(parentZnode, null, CreateMode.PERSISTENT, true, znodeACL).get();
      }
    } catch (ExecutionException ee) {
      // recheck if already created
      throw Throwables.propagate(ee.getCause());
    }
    this.resources = reloadAll();
    notifyListeners();
  }

  private Map<String, T> reloadAll() {
    final Map<String, T> loaded = Maps.newConcurrentMap();
    ZKOperations.watchChildren(zookeeper, parentZnode, new ZKOperations.ChildrenCallback() {
      @Override
      public void updated(NodeChildren nodeChildren) {
        LOG.info("Listing existing children for node " + parentZnode);
        List<String> children = nodeChildren.getChildren();
        for (String child : children) {
          OperationFuture<NodeData> dataFuture = zookeeper.getData(joinZNode(parentZnode, child), watcher);
          final String nodeName = getZNode(dataFuture.getRequestPath());
          Futures.addCallback(dataFuture, new FutureCallback<NodeData>() {
            @Override
            public void onSuccess(NodeData result) {
              LOG.info("Got data for child " + nodeName);
              try {
                T resource = codec.decode(result.getData());
                loaded.put(nodeName, resource);
                for (ResourceListener<T> listener : listeners) {
                  listener.onResourceUpdate(nodeName, resource);
                }
              } catch (IOException ioe) {
                throw Throwables.propagate(ioe);
              }
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.error("Failed to get data for child node " + nodeName, t);
              notifyListenersError(nodeName, t);
            }
          });
          LOG.info("Added future for " + child);
        }
      }
    });

    return loaded;
  }

  /**
   * Adds a {@code ResourceListener} to be notified of cache updates.
   * @param listener the listener to be invoked
   */
  public void addListener(ResourceListener<T> listener) {
    listeners.add(listener);
  }

  /**
   * Removes a previously registered listener from further notifications.
   * @param listener the listener to remove
   * @return whether or not the listener was found and removed
   */
  public boolean removeListener(ResourceListener<T> listener) {
    return listeners.remove(listener);
  }

  private void notifyListeners() {
    for (ResourceListener listener : listeners) {
      listener.onUpdate();
    }
  }

  private void notifyListenersError(String name, Throwable throwable) {
    for (ResourceListener<T> listener : listeners) {
      listener.onError(name, throwable);
    }
  }

  @Override
  public T get(String key) {
    if (key == null) {
      throw new NullPointerException("Key cannot be null.");
    }
    return resources.get(key);
  }

  @Override
  public T getIfPresent(Object key) {
    Preconditions.checkArgument(key instanceof String, "Key must be a String.");
    return get((String) key);
  }

  @Override
  public void put(final String name, final T instance) {
    final String znode = joinZNode(parentZnode, name);
    try {
      final byte[] encoded = codec.encode(instance);
      LOG.info("Setting value for node " + znode);
      OperationFuture<String> future = zookeeper.create(znode, encoded, CreateMode.PERSISTENT);

      Futures.addCallback(future, new FutureCallback<String>() {
        @Override
        public void onSuccess(String result) {
          LOG.info("Created node " + znode);
          resources.put(name, instance);
        }

        @Override
        public void onFailure(Throwable t) {
          if (t instanceof KeeperException.NodeExistsException) {
            LOG.info("Node " + znode + " already exists.  Setting data.");
            OperationFuture<Stat> setDataFuture = zookeeper.setData(znode, encoded);
            Futures.addCallback(setDataFuture, new FutureCallback<Stat>() {
              @Override
              public void onSuccess(Stat result) {
                LOG.info("Set data for node " + znode);
                resources.put(name, instance);
              }

              @Override
              public void onFailure(Throwable t) {
                LOG.error("Failed to set value for node " + znode);
                notifyListenersError(name, t);
              }
            });
          }
        }
      });

    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  /**
   * Removes a resource from the shared cache.
   * @param key the name of the resource to remove
   * @return the previously set resource
   */
  public void remove(Object key) {
    if (key == null) {
      throw new NullPointerException("Key cannot be null.");
    }
    final String name = key.toString();
    final String znode = joinZNode(parentZnode, name);

    OperationFuture<String> future = zookeeper.delete(znode);
    Futures.addCallback(future, new FutureCallback<String>() {
      @Override
      public void onSuccess(String result) {
        LOG.info("Removed value for node {}", znode);
        resources.remove(name);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to remove znode {}", znode);
        notifyListenersError(name, t);
      }
    });
  }

  /**
   * Returns a view of all currently set resources.
   */
  public Iterable<T> getResources() {
    return resources.values();
  }

  @Override
  public long size() {
    return resources.size();
  }

  @Override
  public void putAll(Map<? extends String, ? extends T> map) {
    for (Map.Entry<? extends String, ? extends T> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public boolean equals(Object object) {
    if (!(object instanceof SharedResourceCache)) {
      return false;
    }

    SharedResourceCache other = (SharedResourceCache) object;
    return this.parentZnode.equals(other.parentZnode) &&
      this.resources.equals(other.resources);
  }

  private String joinZNode(String parent, String name) {
    if (parent.endsWith(ZNODE_PATH_SEP)) {
      return parent + name;
    }
    return parent + ZNODE_PATH_SEP + name;
  }

  private String getZNode(String path) {
    return path.substring(path.lastIndexOf("/") + 1);
  }

  private void notifyCreated(final String path) {
    LOG.info("Got created event on " + path);
    final String name = getZNode(path);
    getResource(path, new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        resources.put(name, result);
        for (ResourceListener<T> listener : listeners) {
          listener.onResourceUpdate(name, result);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed updating resource for created znode " + path);
        notifyListenersError(name, t);
      }
    });
  }

  private void notifyDeleted(String path) {
    LOG.info("Got deleted event on " + path);
    String name = getZNode(path);
    resources.remove(name);
    for (ResourceListener<T> listener : listeners) {
      listener.onResourceDelete(name);
    }
  }

  private void notifyChildrenChanged(String path) {
    LOG.info("Got childrenChanged event on " + path);
    if (!path.equals(parentZnode)) {
      LOG.warn("Ignoring children change on znode " + path);
      return;
    }
    resources = reloadAll();
    notifyListeners();
  }

  private void notifyDataChanged(final String path) {
    LOG.info("Got dataChanged event on " + path);
    final String name = getZNode(path);
    getResource(path, new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        resources.put(name, result);
        for (ResourceListener<T> listener : listeners) {
          listener.onResourceUpdate(name, result);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed updating resource for data change on znode " + path);
        notifyListenersError(name, t);
      }
    });
  }

  private void getResource(String path, final FutureCallback<T> resourceCallback) {
    OperationFuture<NodeData> future = zookeeper.getData(path, watcher);
    Futures.addCallback(future, new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        T resource = null;
        try {
          resource = codec.decode(result.getData());
          resourceCallback.onSuccess(resource);
        } catch (IOException ioe) {
          resourceCallback.onFailure(ioe);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        resourceCallback.onFailure(t);
      }
    });
  }

  private class ZKWatcher implements Watcher {
    @Override
    public void process(WatchedEvent event) {
      LOG.info("Watcher got event " + event);
      switch (event.getType()) {
        case None:
          // connection change event
          break;
        case NodeCreated:
          notifyCreated(event.getPath());
          break;
        case NodeDeleted:
          notifyDeleted(event.getPath());
          break;
        case NodeChildrenChanged:
          notifyChildrenChanged(event.getPath());
          break;
        case NodeDataChanged:
          notifyDataChanged(event.getPath());
          break;
      }
    }
  }
}
