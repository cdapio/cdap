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
package org.apache.twill.discovery;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.internal.Constants;
import org.apache.twill.zookeeper.NodeChildren;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKClients;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nullable;

/**
 * Zookeeper implementation of {@link DiscoveryService} and {@link DiscoveryServiceClient}.
 * <p>
 *   Discoverable services are registered within Zookeeper under the namespace 'discoverable' by default.
 *   If you would like to change the namespace under which the services are registered then you can pass
 *   in the namespace during construction of {@link ZKDiscoveryService}.
 * </p>
 *
 * <p>
 *   Following is a simple example of how {@link ZKDiscoveryService} can be used for registering services
 *   and also for discovering the registered services.
 * </p>
 *
 * <blockquote>
 *   <pre>
 *     {@code
 *
 *     DiscoveryService service = new ZKDiscoveryService(zkClient);
 *     service.register(new Discoverable() {
 *       &#64;Override
 *       public String getName() {
 *         return 'service-name';
 *       }
 *
 *       &#64;Override
 *       public InetSocketAddress getSocketAddress() {
 *         return new InetSocketAddress(hostname, port);
 *       }
 *     });
 *     ...
 *     ...
 *     ServiceDiscovered services = service.discovery("service-name");
 *     ...
 *     }
 *   </pre>
 * </blockquote>
 */
public class ZKDiscoveryService implements DiscoveryService, DiscoveryServiceClient, AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ZKDiscoveryService.class);

  private static final long RETRY_MILLIS = 1000;

  private final AtomicBoolean closed;
  // In memory map for recreating ephemeral nodes after session expires.
  // It map from discoverable to the corresponding Cancellable
  private final Multimap<Discoverable, DiscoveryCancellable> discoverables;
  private final Lock lock;

  private final LoadingCache<String, ServiceDiscoveredCacheEntry> services;
  private final ZKClient zkClient;
  private final ScheduledExecutorService retryExecutor;
  private final Cancellable watcherCancellable;

  /**
   * Constructs ZKDiscoveryService using the provided zookeeper client for storing service registry.
   * @param zkClient The {@link ZKClient} for interacting with zookeeper.
   */
  public ZKDiscoveryService(ZKClient zkClient) {
    this(zkClient, Constants.DISCOVERY_PATH_PREFIX);
  }

  /**
   * Constructs ZKDiscoveryService using the provided zookeeper client for storing service registry under namespace.
   * @param zkClient of zookeeper quorum
   * @param namespace under which the service registered would be stored in zookeeper.
   *                  If namespace is {@code null}, no namespace will be used.
   */
  public ZKDiscoveryService(ZKClient zkClient, String namespace) {
    this.closed = new AtomicBoolean();
    this.discoverables = HashMultimap.create();
    this.lock = new ReentrantLock();
    this.retryExecutor = Executors.newSingleThreadScheduledExecutor(
      Threads.createDaemonThreadFactory("zk-discovery-retry"));
    this.zkClient = namespace == null ? zkClient : ZKClients.namespace(zkClient, namespace);
    this.services = CacheBuilder.newBuilder()
      .removalListener(new RemovalListener<String, ServiceDiscoveredCacheEntry>() {
        @Override
        public void onRemoval(RemovalNotification<String, ServiceDiscoveredCacheEntry> notification) {
          ServiceDiscoveredCacheEntry entry = notification.getValue();
          if (entry != null) {
            entry.cancel();
          }
        }
      })
      .build(createServiceLoader());
    this.watcherCancellable = this.zkClient.addConnectionWatcher(createConnectionWatcher());
  }

  /**
   * Registers a {@link Discoverable} in zookeeper.
   * <p>
   *   Registering a {@link Discoverable} will create a node &lt;base&gt;/&lt;service-name&gt;
   *   in zookeeper as a ephemeral node. If the node already exists (timeout associated with emphemeral node creation), 
   *   then a runtime exception is thrown to make sure that a service with an intent to register is not started without 
   *   registering. 
   *   When a runtime exception is thrown, expectation is that the process being started will fail and would be started 
   *   again by the monitoring service.
   * </p>
   * @param discoverable Information of the service provider that could be discovered.
   * @return An instance of {@link Cancellable}
   */
  @Override
  public Cancellable register(final Discoverable discoverable) {
    if (closed.get()) {
      throw new IllegalStateException("Cannot register discoverable through a closed ZKDiscoveryService");
    }

    final SettableFuture<String> future = SettableFuture.create();
    final DiscoveryCancellable cancellable = new DiscoveryCancellable(discoverable);

    // Create the zk ephemeral node.
    Futures.addCallback(doRegister(discoverable), new FutureCallback<String>() {
      @Override
      public void onSuccess(String result) {
        // Set the sequence node path to cancellable for future cancellation.
        cancellable.setPath(result);
        lock.lock();
        try {
          if (!closed.get()) {
            discoverables.put(discoverable, cancellable);
          } else {
            cancellable.asyncCancel();
          }
        } finally {
          lock.unlock();
        }
        LOG.debug("Service registered: {} {}", discoverable, result);
        future.set(result);
      }

      @Override
      public void onFailure(Throwable t) {
        if (t instanceof KeeperException.NodeExistsException) {
          handleRegisterFailure(discoverable, future, this, t);
        } else {
          LOG.warn("Failed to register: {}", discoverable, t);
          future.setException(t);
        }
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    Futures.getUnchecked(future);
    return cancellable;
  }

  @Override
  public ServiceDiscovered discover(String service) {
    if (closed.get()) {
      throw new IllegalStateException("Cannot discover through a closed ZKDiscoveryService");
    }
    return services.getUnchecked(service);
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    // Stop the registration retry executor
    retryExecutor.shutdownNow();

    // Cancel the connection watcher
    watcherCancellable.cancel();

    // Cancel all registered services
    List<ListenableFuture<?>> futures = new ArrayList<>();
    lock.lock();
    try {
      for (Map.Entry<Discoverable, DiscoveryCancellable> entry : discoverables.entries()) {
        LOG.debug("Un-registering service {} - {}", entry.getKey().getName(), entry.getKey().getSocketAddress());
        futures.add(entry.getValue().asyncCancel());
      }
    } finally {
      lock.unlock();
    }
    try {
      Futures.successfulAsList(futures).get();
      LOG.debug("All services unregistered");
    } catch (Exception e) {
      // This is not expected to happen
      LOG.warn("Unexpected exception when waiting for all services to get unregistered", e);
    }

    // Cancel all services being watched
    services.invalidateAll();
  }

  /**
   * Handle registration failure.
   *
   * @param discoverable The discoverable to register.
   * @param completion A settable future to set when registration is completed / failed.
   * @param creationCallback A future callback for path creation.
   * @param failureCause The original cause of failure.
   */
  private void handleRegisterFailure(final Discoverable discoverable,
                                     final SettableFuture<String> completion,
                                     final FutureCallback<String> creationCallback,
                                     final Throwable failureCause) {
    if (closed.get()) {
      return;
    }

    final String path = getNodePath(discoverable);
    Futures.addCallback(zkClient.exists(path), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(@Nullable Stat result) {
        if (result == null) {
          // If the node is gone, simply retry.
          LOG.info("Node {} is gone. Retry registration for {}.", path, discoverable);
          retryRegister(discoverable, creationCallback);
          return;
        }

        long ephemeralOwner = result.getEphemeralOwner();
        if (ephemeralOwner == 0) {
          // it is not an ephemeral node, something wrong.
          LOG.error("Node {} already exists and is not an ephemeral node. Discoverable registration failed: {}.",
                    path, discoverable);
          completion.setException(failureCause);
          return;
        }
        Long sessionId = zkClient.getSessionId();
        if (sessionId == null || ephemeralOwner != sessionId) {
          // This zkClient is not valid or doesn't own the ephemeral node, simply keep retrying.
          LOG.info("Owner of {} is different. Retry registration for {}.", path, discoverable);
          retryRegister(discoverable, creationCallback);
        } else {
          // This client owned the node, treat the registration as completed.
          // This could happen if same client tries to register twice (due to mistake or failure race condition).
          completion.set(path);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // If exists call failed, simply retry creation.
        LOG.warn("Error when getting stats on {}. Retry registration for {}.", path, discoverable);
        retryRegister(discoverable, creationCallback);
      }
    }, Threads.SAME_THREAD_EXECUTOR);
  }

  private OperationFuture<String> doRegister(Discoverable discoverable) {
    byte[] discoverableBytes = DiscoverableAdapter.encode(discoverable);
    return zkClient.create(getNodePath(discoverable), discoverableBytes, CreateMode.EPHEMERAL, true);
  }

  private void retryRegister(final Discoverable discoverable, final FutureCallback<String> creationCallback) {
    if (closed.get()) {
      return;
    }

    retryExecutor.schedule(new Runnable() {

      @Override
      public void run() {
        if (!closed.get()) {
          Futures.addCallback(doRegister(discoverable), creationCallback, Threads.SAME_THREAD_EXECUTOR);
        }
      }
    }, RETRY_MILLIS, TimeUnit.MILLISECONDS);
  }

  /**
   * Generate unique node path for a given {@link Discoverable}.
   * @param discoverable An instance of {@link Discoverable}.
   * @return A node name based on the discoverable.
   */
  private String getNodePath(Discoverable discoverable) {
    InetSocketAddress socketAddress = discoverable.getSocketAddress();
    String node = Hashing.md5()
                         .newHasher()
                         .putBytes(socketAddress.getAddress().getAddress())
                         .putInt(socketAddress.getPort())
                         .hash().toString();

    return String.format("/%s/%s", discoverable.getName(), node);
  }

  private Watcher createConnectionWatcher() {
    return new Watcher() {
      // Watcher is invoked from single event thread, hence safe to use normal mutable variable.
      private boolean expired;

      @Override
      public void process(WatchedEvent event) {
        if (event.getState() == Event.KeeperState.Expired) {
          LOG.warn("ZK Session expired: {}", zkClient.getConnectString());
          expired = true;
        } else if (event.getState() == Event.KeeperState.SyncConnected && expired) {
          LOG.info("Reconnected after expiration: {}", zkClient.getConnectString());
          expired = false;

          // Re-register all services
          lock.lock();
          try {
            for (final Map.Entry<Discoverable, DiscoveryCancellable> entry : discoverables.entries()) {
              if (closed.get()) {
                entry.getValue().asyncCancel();
                continue;
              }

              LOG.info("Re-registering service: {}", entry.getKey());

              // Must be non-blocking in here.
              Futures.addCallback(doRegister(entry.getKey()), new FutureCallback<String>() {
                @Override
                public void onSuccess(String result) {
                  // Updates the cancellable to the newly created sequential node.
                  entry.getValue().setPath(result);
                  LOG.debug("Service re-registered: {} {}", entry.getKey(), result);
                }

                @Override
                public void onFailure(Throwable t) {
                  // When failed to create the node, there would be no retry and simply make the cancellable do nothing.
                  entry.getValue().setPath(null);
                  LOG.error("Failed to re-register service: {}", entry.getKey(), t);
                }
              }, Threads.SAME_THREAD_EXECUTOR);
            }
          } finally {
            lock.unlock();
          }
        }
      }
    };
  }

  /**
   * Creates a CacheLoader for creating live Iterable for watching instances changes for a given service.
   */
  private CacheLoader<String, ServiceDiscoveredCacheEntry> createServiceLoader() {
    return new CacheLoader<String, ServiceDiscoveredCacheEntry>() {
      @Override
      public ServiceDiscoveredCacheEntry load(String service) throws Exception {
        final DefaultServiceDiscovered serviceDiscovered = new DefaultServiceDiscovered(service);
        final String pathBase = "/" + service;

        // Watch for children changes in /service
        Cancellable cancellable = ZKOperations.watchChildren(zkClient, pathBase, new ZKOperations.ChildrenCallback() {
          @Override
          public void updated(NodeChildren nodeChildren) {
            // Fetch data of all children nodes in parallel.
            List<String> children = nodeChildren.getChildren();
            List<OperationFuture<NodeData>> dataFutures = Lists.newArrayListWithCapacity(children.size());
            for (String child : children) {
              dataFutures.add(zkClient.getData(pathBase + "/" + child));
            }

            // Update the service map when all fetching are done.
            final ListenableFuture<List<NodeData>> fetchFuture = Futures.successfulAsList(dataFutures);
            fetchFuture.addListener(new Runnable() {
              @Override
              public void run() {
                ImmutableSet.Builder<Discoverable> builder = ImmutableSet.builder();
                for (NodeData nodeData : Futures.getUnchecked(fetchFuture)) {
                  // For successful fetch, decode the content.
                  if (nodeData != null) {
                    Discoverable discoverable = DiscoverableAdapter.decode(nodeData.getData());
                    if (discoverable != null) {
                      builder.add(discoverable);
                    }
                  }
                }
                serviceDiscovered.setDiscoverables(builder.build());
              }
            }, Threads.SAME_THREAD_EXECUTOR);
          }
        });
        return new ServiceDiscoveredCacheEntry(serviceDiscovered, cancellable);
      }
    };
  }

  /**
   * Inner class for cancelling (un-register) discovery service.
   */
  private final class DiscoveryCancellable implements Cancellable {

    private final Discoverable discoverable;
    private final AtomicBoolean cancelled;
    private volatile String path;

    DiscoveryCancellable(Discoverable discoverable) {
      this.discoverable = discoverable;
      this.cancelled = new AtomicBoolean();
    }

    /**
     * Set the zk node path representing the ephemeral sequence node of this registered discoverable.
     * Called from ZK event thread when creating of the node completed, either from normal registration or
     * re-registration due to session expiration.
     *
     * @param path The path to ephemeral sequence node.
     */
    void setPath(String path) {
      this.path = path;
      if (cancelled.get() && path != null) {
        // Simply delete the path if it's already cancelled
        // It's for the case when session expire happened and re-registration completed after this has been cancelled.
        // Not bother with the result as if there is error, nothing much we could do.
        zkClient.delete(path);
      }
    }

    @Override
    public void cancel() {
      Futures.getUnchecked(asyncCancel());
      LOG.debug("Service unregistered: {} {}", discoverable, path);
    }

    ListenableFuture<?> asyncCancel() {
      if (!cancelled.compareAndSet(false, true)) {
        return Futures.immediateFuture(null);
      }

      // Take a snapshot of the volatile path.
      String path = this.path;

      // If it is null, meaning cancel is called before the ephemeral node is created, hence
      // setPath() will be called in future (through zk callback when creation is completed)
      // so that deletion will be done in setPath().
      if (path == null) {
        return Futures.immediateFuture(null);
      }

      // Remove this Cancellable from the map so that upon session expiration won't try to register.
      lock.lock();
      try {
        discoverables.remove(discoverable, this);
      } finally {
        lock.unlock();
      }

      // Delete the path. It's ok if the path not exists
      // (e.g. what session expired and before node has been re-created)
      try {
        return ZKOperations.ignoreError(zkClient.delete(path), KeeperException.NoNodeException.class, path);
      } catch (Exception e) {
        return Futures.immediateFailedFuture(e);
      }
    }
  }

  /**
   * Class to be used as the service discovered cache entry.
   */
  private static final class ServiceDiscoveredCacheEntry implements Cancellable, ServiceDiscovered {
    private final ServiceDiscovered serviceDiscovered;
    private final Cancellable cancellable;

    private ServiceDiscoveredCacheEntry(ServiceDiscovered serviceDiscovered, Cancellable cancellable) {
      this.serviceDiscovered = serviceDiscovered;
      this.cancellable = cancellable;
    }

    @Override
    public void cancel() {
      cancellable.cancel();
    }

    @Override
    public String getName() {
      return serviceDiscovered.getName();
    }

    @Override
    public Cancellable watchChanges(ChangeListener listener, Executor executor) {
      return serviceDiscovered.watchChanges(listener, executor);
    }

    @Override
    public boolean contains(Discoverable discoverable) {
      return serviceDiscovered.contains(discoverable);
    }

    @Override
    public Iterator<Discoverable> iterator() {
      return serviceDiscovered.iterator();
    }
  }
}

