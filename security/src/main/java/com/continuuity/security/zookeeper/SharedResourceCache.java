package com.continuuity.security.zookeeper;

import com.continuuity.api.common.Bytes;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.zookeeper.ZKExtOperations;
import com.continuuity.security.io.Codec;
import com.google.common.base.Throwables;
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
public class SharedResourceCache<T> extends AbstractIdleService implements Map<String, T> {
  private static final String ZNODE_PATH_SEP = "/";
  private static final int MAX_RETRIES = 3;
  private static final Logger LOG = LoggerFactory.getLogger(SharedResourceCache.class);

  private final List<ACL> znodeACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;
  //private final List<ACL> znodeACL = ZooDefs.Ids.CREATOR_ALL_ACL;
  private final ZKClient zookeeper;
  private final Codec<T> codec;
  private final String parentZnode;
  private final List<ResourceListener> listeners = Lists.newArrayList();
  private ZKWatcher watcher;
  private Map<String, T> resources;
  private Lock lock = new ReentrantLock();

  @Inject
  public SharedResourceCache(CConfiguration cConf, ZKClient zookeeper, Codec<T> codec) {
    this(zookeeper, codec, cConf.get(Constants.Security.DIST_KEY_PARENT_ZNODE));
  }

  public SharedResourceCache(ZKClient zookeeper, Codec<T> codec, String parentZnode) {
    this.zookeeper = zookeeper;
    this.codec = codec;
    this.parentZnode = parentZnode;
  }

  @Override
  protected void startUp() throws Exception {
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

  @Override
  protected void shutDown() throws Exception {

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
              } catch (IOException ioe) {
                throw Throwables.propagate(ioe);
              }
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.error("Failed to get data for child node " + nodeName, t);
            }
          });
          LOG.info("Added future for " + child);
        }
      }
    });

    return loaded;
  }

  public void addListener(ResourceListener listener) {
    listeners.add(listener);
  }

  private void notifyListeners() {
    for (ResourceListener listener : listeners) {
      listener.onUpdate();
    }
  }

  @Override
  public T get(Object key) {
    if (key == null) {
      throw new NullPointerException("Key cannot be null.");
    }
    String name = key.toString();
    return resources.get(name);
  }

  @Override
  public T put(String name, T instance) {
    String znode = joinZNode(parentZnode, name);
    try {
      byte[] encoded = codec.encode(instance);
      try {
        lock.lock();
        LOG.info("Setting value for node " + znode);
        OperationFuture<String> future = zookeeper.create(znode, encoded, CreateMode.PERSISTENT);
        Futures.getUnchecked(future);
        LOG.info("Set value for node " + znode);
        return resources.put(name, instance);
      } finally {
        lock.unlock();
      }
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  @Override
  public T remove(Object key) {
    if (key == null) {
      throw new NullPointerException("Key cannot be null.");
    }
    String name = key.toString();
    String znode = joinZNode(parentZnode, name);
    T removedInstance = null;
    try {
      lock.lock();
      OperationFuture<String> future = zookeeper.delete(znode);
      Futures.getUnchecked(future);
      LOG.info("Removed value for node " + znode);
      removedInstance = resources.remove(name);
      return removedInstance;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int size() {
    return resources.size();
  }

  @Override
  public boolean isEmpty() {
    return resources.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return resources.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return resources.containsValue(value);
  }

  @Override
  public void putAll(Map<? extends String, ? extends T> map) {
    for (Map.Entry<? extends String, ? extends T> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Set<String> keySet() {
    return resources.keySet();
  }

  @Override
  public Collection<T> values() {
    return resources.values();
  }

  @Override
  public Set<Entry<String, T>> entrySet() {
    return resources.entrySet();
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

  private void notifyCreated(String path) {
    LOG.info("Got created event on " + path);
    String name = getZNode(path);
    try {
      lock.lock();
      T resource = getResource(path);
      resources.put(name, resource);
    } finally {
      lock.unlock();
    }
  }

  private void notifyDeleted(String path) {
    LOG.info("Got deleted event on " + path);
    String name = getZNode(path);
    try {
      lock.lock();
      resources.remove(name);
    } finally {
      lock.unlock();
    }
  }

  private void notifyChildrenChanged(String path) {
    LOG.info("Got childrenChanged event on " + path);
    if (!path.equals(parentZnode)) {
      LOG.warn("Ignoring children change on znode " + path);
      return;
    }
    try {
      lock.lock();
      resources = reloadAll();
    } finally {
      lock.unlock();
    }
    notifyListeners();
  }

  private void notifyDataChanged(String path) {
    LOG.info("Got dataChanged event on " + path);
    String name = getZNode(path);
    try {
      lock.lock();
      T instance = getResource(path);
      resources.put(name, instance);
    } finally {
      lock.unlock();
    }
  }

  private T getResource(String path) {
    OperationFuture<NodeData> future = zookeeper.getData(path, watcher);
    T resource = null;
    try {
     resource = codec.decode(Futures.getUnchecked(future).getData());
    } catch (IOException ioe) {
      throw Throwables.propagate(ioe);
    }
    return resource;
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
