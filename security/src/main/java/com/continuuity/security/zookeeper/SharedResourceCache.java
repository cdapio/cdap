package com.continuuity.security.zookeeper;

import com.continuuity.common.io.Codec;
import com.continuuity.common.zookeeper.ZKExtOperations;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.AbstractLoadingCache;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
  private ZKWatcher watcher;
  private Map<String, T> resources;
  private ListenerManager listeners;

  public SharedResourceCache(ZKClient zookeeper, Codec<T> codec, String parentZnode) {
    this.zookeeper = zookeeper;
    this.codec = codec;
    this.parentZnode = parentZnode;
    this.listeners = new ListenerManager();
  }

  public void init() throws InterruptedException {
    this.watcher = new ZKWatcher();
    try {
      LOG.info("Initializing SharedResourceCache.  Checking for parent znode {}", parentZnode);
      if (zookeeper.exists(parentZnode).get() == null) {
        // may be created in parallel by another instance
        ZKOperations.ignoreError(zookeeper.create(parentZnode, null, CreateMode.PERSISTENT, true, znodeACL),
                                 KeeperException.NodeExistsException.class, null).get();
      }
    } catch (ExecutionException ee) {
      // recheck if already created
      throw Throwables.propagate(ee.getCause());
    }
    this.resources = reloadAll();
    listeners.notifyUpdate();
  }

  private Map<String, T> reloadAll() {
    final Map<String, T> loaded = Maps.newConcurrentMap();
    ZKOperations.watchChildren(zookeeper, parentZnode, new ZKOperations.ChildrenCallback() {
      @Override
      public void updated(NodeChildren nodeChildren) {
        LOG.info("Listing existing children for node {}", parentZnode);
        List<String> children = nodeChildren.getChildren();
        for (String child : children) {
          OperationFuture<NodeData> dataFuture = zookeeper.getData(joinZNode(parentZnode, child), watcher);
          final String nodeName = getZNode(dataFuture.getRequestPath());
          Futures.addCallback(dataFuture, new FutureCallback<NodeData>() {
            @Override
            public void onSuccess(NodeData result) {
              LOG.debug("Got data for child {}", nodeName);
              try {
                final T resource = codec.decode(result.getData());
                loaded.put(nodeName, resource);
                listeners.notifyResourceUpdate(nodeName, resource);
              } catch (IOException ioe) {
                throw Throwables.propagate(ioe);
              }
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.error("Failed to get data for child node {}", nodeName, t);
              listeners.notifyError(nodeName, t);
            }
          });
          LOG.debug("Added future for {}", child);
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
      LOG.debug("Setting value for node {}", znode);
      ListenableFuture<String> future = ZKExtOperations.createOrSet(zookeeper, znode, encoded, znode,
                                                                    MAX_RETRIES, znodeACL);

      Futures.addCallback(future, new FutureCallback<String>() {
        @Override
        public void onSuccess(String result) {
          LOG.debug("Created or set node {}", znode);
          resources.put(name, instance);
        }

        @Override
        public void onFailure(Throwable t) {
          LOG.error("Failed to set value for node {}", znode, t);
          listeners.notifyError(name, t);
        }
      });

    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  /**
   * Removes a resource from the shared cache.
   * @param key the name of the resource to remove
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
        LOG.debug("Removed value for node {}", znode);
        resources.remove(name);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed to remove znode {}", znode, t);
        listeners.notifyError(name, t);
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
    LOG.debug("Got created event on {}", path);
    final String name = getZNode(path);
    getResource(path, new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        resources.put(name, result);
        listeners.notifyResourceUpdate(name, result);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed updating resource for created znode {}", path, t);
        listeners.notifyError(name, t);
      }
    });
  }

  private void notifyDeleted(String path) {
    LOG.debug("Got deleted event on {}", path);
    String name = getZNode(path);
    resources.remove(name);
    listeners.notifyDelete(name);
  }

  private void notifyChildrenChanged(String path) {
    LOG.debug("Got childrenChanged event on {}", path);
    if (!path.equals(parentZnode)) {
      LOG.warn("Ignoring children change on znode {}", path);
      return;
    }
    resources = reloadAll();
    listeners.notifyUpdate();
  }

  private void notifyDataChanged(final String path) {
    LOG.debug("Got dataChanged event on {}", path);
    final String name = getZNode(path);
    getResource(path, new FutureCallback<T>() {
      @Override
      public void onSuccess(T result) {
        resources.put(name, result);
        listeners.notifyResourceUpdate(name, result);
      }

      @Override
      public void onFailure(Throwable t) {
        LOG.error("Failed updating resource for data change on znode {}", path, t);
        listeners.notifyError(name, t);
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
      LOG.debug("Watcher got event {}", event);
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

  private class ListenerManager {
    private final Set<ResourceListener<T>> listeners = Sets.newCopyOnWriteArraySet();
    private ExecutorService listenerExecutor;

    private ListenerManager() {
      this.listenerExecutor = Executors.newSingleThreadExecutor(
        Threads.createDaemonThreadFactory("SharedResourceCache-listener-%d"));
    }

    private void add(ResourceListener<T> listener) {
      this.listeners.add(listener);
    }

    private boolean remove(ResourceListener<T> listener) {
      return this.listeners.remove(listener);
    }

    private void notifyUpdate() {
      listenerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          for (ResourceListener listener : listeners) {
            try {
              listener.onUpdate();
            } catch (Throwable t) {
              LOG.error("Exception notifying listener {}", listener, t);
              Throwables.propagateIfInstanceOf(t, Error.class);
            }
          }
        }
      });
    }

    private void notifyResourceUpdate(final String name, final T resource) {
      listenerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          for (ResourceListener<T> listener : listeners) {
            try {
              listener.onResourceUpdate(name, resource);
            } catch (Throwable t) {
              LOG.error("Exception notifying listener {}", listener, t);
              Throwables.propagateIfInstanceOf(t, Error.class);
            }
          }
        }
      });
    }

    private void notifyDelete(final String name) {
      listenerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          for (ResourceListener<T> listener : listeners) {
            try {
              listener.onResourceDelete(name);
            } catch (Throwable t) {
              LOG.error("Exception notifying listener {}", listener, t);
              Throwables.propagateIfInstanceOf(t, Error.class);
            }
          }
        }
      });
    }

    private void notifyError(final String name, final Throwable throwable) {
      listenerExecutor.submit(new Runnable() {
        @Override
        public void run() {
          for (ResourceListener<T> listener : listeners) {
            try {
              listener.onError(name, throwable);
            } catch (Throwable t) {
              LOG.error("Exception notifying listener {}", listener, t);
              Throwables.propagateIfInstanceOf(t, Error.class);
            }
          }
        }
      });
    }
  }
}
