/*
 * Copyright © 2014 Cask Data, Inc.
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

package io.cdap.cdap.security.zookeeper;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.cache.AbstractLoadingCache;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.common.zookeeper.ZKExtOperations;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.ACLData;
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

/**
 * ZooKeeper recipe to propagate changes to a shared cache across a number of listeners.  The cache
 * entries are materialized as child znodes under a common parent.
 *
 * @param <T> The type of resource that is distributed to all participants in the cache.
 */
public class SharedResourceCache<T> extends AbstractLoadingCache<String, T> {

  private static final String ZNODE_PATH_SEP = "/";
  private static final int MAX_RETRIES = 3;
  private static final Logger LOG = LoggerFactory.getLogger(SharedResourceCache.class);

  private final List<ACL> znodeACL;

  private final ZKClient zookeeper;
  private final Codec<T> codec;
  private final String parentZnode;
  private ZKWatcher watcher;
  private Map<String, T> resources;
  private ListenerManager listeners;

  public SharedResourceCache(ZKClient zookeeper, Codec<T> codec, String parentZnode,
      List<ACL> znodeACL) {
    this.zookeeper = zookeeper;
    this.codec = codec;
    this.parentZnode = parentZnode;
    this.znodeACL = znodeACL;
    this.listeners = new ListenerManager();
  }

  /**
   * Initializes the parent znode if absent, restricts it if an older version left it
   * world-writable, and starts watching child znodes for updates.
   */
  public void init() throws InterruptedException {
    this.watcher = new ZKWatcher();
    try {
      LOG.info("Initializing SharedResourceCache.  Checking for parent znode {}", parentZnode);
      createParentIfAbsent();
    } catch (ExecutionException ee) {
      // Sessions without ZooKeeper credentials (e.g. YARN program containers) get InvalidACL for
      // "auth"-scheme ACLs. Tolerate only that so they can still start; rethrow anything else.
      if (!(ee.getCause() instanceof KeeperException.InvalidACLException) || !hasAuthSchemeAcl()) {
        throw Throwables.propagate(ee.getCause());
      }
      // Never fall back to an open parent: that is the vulnerability this ACL closes.
      LOG.warn("ZooKeeper rejected ACLs {} for parent znode {} because this session is not "
          + "authenticated. Not creating it; entries will only load once an authenticated "
          + "client creates it with ACLs readable by this session.", znodeACL, parentZnode);
    }
    restrictOpenParentAcl();
    this.resources = reloadAll();
    listeners.notifyUpdate();
  }

  /**
   * Restricts an existing parent znode that older versions created world-writable, so that no
   * further entries can be added by unauthorized clients on upgraded clusters. Entries added while
   * the parent was writable are kept. Only an ACL of exactly {@code OPEN_ACL_UNSAFE} is replaced;
   * any other ACL may have been set by an operator and is left unchanged. A failure here is logged
   * and never stops initialization.
   */
  private void restrictOpenParentAcl() throws InterruptedException {
    if (znodeACL == null || znodeACL.isEmpty() || ZooDefs.Ids.OPEN_ACL_UNSAFE.equals(znodeACL)) {
      return;
    }
    ACLData aclData;
    try {
      aclData = zookeeper.getACL(parentZnode).get();
    } catch (ExecutionException ee) {
      // The parent is missing because this session could not create it, or is not readable.
      LOG.debug("Not checking ACLs of parent znode {}", parentZnode, ee.getCause());
      return;
    }
    if (!ZooDefs.Ids.OPEN_ACL_UNSAFE.equals(aclData.getACL())) {
      return;
    }
    try {
      // The ACL version guards against overwriting a concurrent change by another instance.
      Stat stat = ZKOperations.ignoreError(
          zookeeper.setACL(parentZnode, znodeACL, aclData.getStat().getAversion()),
          KeeperException.BadVersionException.class, null).get();
      if (stat == null) {
        LOG.debug("ACLs of parent znode {} were changed concurrently; not changing them",
            parentZnode);
        return;
      }
      LOG.warn("Restricted ACLs of parent znode {} from {} to {}. Older versions created it "
          + "writable by any ZooKeeper client. Its {} existing entries were kept; review them, as "
          + "they may have been added while it was writable.", parentZnode,
          ZooDefs.Ids.OPEN_ACL_UNSAFE, describeParentAcl(), aclData.getStat().getNumChildren());
    } catch (ExecutionException ee) {
      if (ee.getCause() instanceof KeeperException.InvalidACLException && hasAuthSchemeAcl()) {
        LOG.info("Parent znode {} is writable by any ZooKeeper client. This session is not "
            + "authenticated, so an authenticated CDAP service must restrict its ACLs to {}.",
            parentZnode, znodeACL);
        return;
      }
      LOG.warn("Parent znode {} is writable by any ZooKeeper client and its ACLs could not be "
          + "restricted to {}. Restrict them manually, for example with zkCli.sh setAcl.",
          parentZnode, znodeACL, ee.getCause());
    }
  }

  /**
   * Returns the ACLs now set on the parent znode, which show the identity they were restricted to,
   * or the configured ACLs if they cannot be read.
   */
  private String describeParentAcl() throws InterruptedException {
    try {
      return zookeeper.getACL(parentZnode).get().getACL().toString();
    } catch (ExecutionException ee) {
      return String.valueOf(znodeACL);
    }
  }

  private void createParentIfAbsent() throws ExecutionException, InterruptedException {
    if (zookeeper.exists(parentZnode).get() == null) {
      // Create the parent with znodeACL, the same ACLs as the entries under it, so only
      // authorized clients can add entries. Another instance may create it at the same time, so
      // NodeExistsException is ignored. restrictOpenParentAcl() handles an existing open parent.
      OperationFuture<String> createFuture = (znodeACL == null)
          ? zookeeper.create(parentZnode, null, CreateMode.PERSISTENT)
          : zookeeper.create(parentZnode, null, CreateMode.PERSISTENT, znodeACL);
      ZKOperations.ignoreError(createFuture,
          KeeperException.NodeExistsException.class, null).get();
    }
  }

  private boolean hasAuthSchemeAcl() {
    if (znodeACL == null) {
      return false;
    }
    for (ACL acl : znodeACL) {
      if (acl != null && acl.getId() != null && "auth".equals(acl.getId().getScheme())) {
        return true;
      }
    }
    return false;
  }

  private Map<String, T> reloadAll() {
    final Map<String, T> loaded = Maps.newConcurrentMap();
    ZKOperations.watchChildren(zookeeper, parentZnode, new ZKOperations.ChildrenCallback() {
      @Override
      public void updated(NodeChildren nodeChildren) {
        LOG.info("Listing existing children for node {}", parentZnode);
        List<String> children = nodeChildren.getChildren();
        for (String child : children) {
          OperationFuture<NodeData> dataFuture = zookeeper.getData(joinZNode(parentZnode, child),
              watcher);
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
   *
   * @param listener the listener to be invoked
   */
  public void addListener(ResourceListener<T> listener) {
    listeners.add(listener);
  }

  /**
   * Removes a previously registered listener from further notifications.
   *
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
      LOG.debug("Setting value for node {}", znode);
      // Ensure parentZnode exists with znodeACL before ZKExtOperations.createOrSet runs;
      // otherwise ZKExtOperations would recreate a missing parent with OPEN_ACL_UNSAFE.
      createParentIfAbsent();

      final SettableFuture<T> completion = SettableFuture.create();
      Futures.addCallback(
          ZKExtOperations.createOrSet(zookeeper, znode, Suppliers.ofInstance(instance), codec,
              MAX_RETRIES, znodeACL),
          new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
              LOG.debug("Created or set node {}", znode);
              resources.put(name, result);
              completion.set(result);
            }

            @Override
            public void onFailure(Throwable t) {
              LOG.error("Failed to set value for node {}", znode, t);
              listeners.notifyError(name, t);
              completion.setException(t);
            }
          }
      );

      // Block until it is done
      completion.get();

    } catch (Exception ioe) {
      throw Throwables.propagate(ioe);
    }
  }

  /**
   * Removes a resource from the shared cache.
   *
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
    return Objects.equals(this.parentZnode, other.parentZnode)
        && Objects.equals(this.resources, other.resources);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parentZnode, resources);
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
