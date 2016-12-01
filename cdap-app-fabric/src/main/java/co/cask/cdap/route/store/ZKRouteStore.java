/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.route.store;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.io.Codec;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.common.zookeeper.ZKExtOperations;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.annotation.Nullable;

/**
 * RouteStore where the routes are stored in a ZK persistent node. This is intended for use in distributed mode.
 */
public class ZKRouteStore implements RouteStore {
  private static final Logger LOG = LoggerFactory.getLogger(ZKRouteStore.class);
  private static final Gson GSON = new Gson();
  private static final int ZK_TIMEOUT_SECS = 5;
  private static final Codec<RouteConfig> ROUTE_CONFIG_CODEC = new Codec<RouteConfig>() {

    @Override
    public byte[] encode(RouteConfig object) throws IOException {
      return Bytes.toBytes(GSON.toJson(object));
    }

    @Override
    public RouteConfig decode(byte[] data) throws IOException {
      return GSON.fromJson(Bytes.toString(data), RouteConfig.class);
    }
  };

  private final ZKClient zkClient;
  private final ConcurrentMap<ProgramId, SettableFuture<RouteConfig>> routeConfigMap;


  @Inject
  public ZKRouteStore(ZKClient zkClient) {
    this.zkClient = zkClient;
    this.routeConfigMap = Maps.newConcurrentMap();
  }

  @Override
  public void store(final ProgramId serviceId, final RouteConfig routeConfig) {
    Supplier<RouteConfig> supplier = Suppliers.ofInstance(routeConfig);
    SettableFuture<RouteConfig> oldConfigFuture = routeConfigMap.get(serviceId);
    Future<RouteConfig> future = ZKExtOperations.createOrSet(zkClient, getZKPath(serviceId), supplier,
                                                             ROUTE_CONFIG_CODEC, 10);
    try {
      future.get(ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
      SettableFuture<RouteConfig> newFuture = SettableFuture.create();
      newFuture.set(routeConfig);

      if (oldConfigFuture != null) {
        routeConfigMap.replace(serviceId, oldConfigFuture, newFuture);
      } else {
        routeConfigMap.putIfAbsent(serviceId, newFuture);
      }
    } catch (ExecutionException | InterruptedException | TimeoutException ex) {
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public void delete(final ProgramId serviceId) throws NotFoundException {
    OperationFuture<String> future = zkClient.delete(getZKPath(serviceId));
    SettableFuture<RouteConfig> oldConfigFuture = routeConfigMap.get(serviceId);
    try {
      future.get(ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
      routeConfigMap.remove(serviceId, oldConfigFuture);
    } catch (ExecutionException | InterruptedException | TimeoutException ex) {
      if (ex.getCause() instanceof KeeperException.NoNodeException) {
        throw new NotFoundException(String.format("Route Config for Service %s was not found.", serviceId));
      }
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public RouteConfig fetch(final ProgramId serviceId) {
    Future<RouteConfig> future = routeConfigMap.get(serviceId);
    if (future == null) {
      SettableFuture<RouteConfig> settableFuture = SettableFuture.create();
      future = routeConfigMap.putIfAbsent(serviceId, settableFuture);
      if (future == null) {
        future = getAndWatchData(serviceId, settableFuture, settableFuture, new ZKRouteWatcher(serviceId));
      }
    }
    return getConfig(serviceId, future);
  }

  private RouteConfig getConfig(ProgramId serviceId, Future<RouteConfig> future) {
    try {
      return future.get(ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.debug("Getting configuration for service {} from ZK failed.", serviceId, e);
      return new RouteConfig(Collections.<String, Integer>emptyMap());
    }
  }

  private static String getZKPath(ProgramId serviceId) {
    return String.format("/routestore/%s", ServiceDiscoverable.getName(serviceId));
  }

  private Future<RouteConfig> getAndWatchData(final ProgramId serviceId,
                                              final SettableFuture<RouteConfig> settableFuture,
                                              final SettableFuture<RouteConfig> oldSettableFuture,
                                              final Watcher watcher) {
    OperationFuture<NodeData> future = zkClient.getData(getZKPath(serviceId), watcher);
    Futures.addCallback(future, new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        try {
          RouteConfig route = ROUTE_CONFIG_CODEC.decode(result.getData());
          settableFuture.set(route);

          // Replace the future in the routeConfigMap in order to reflect the route config changes
          routeConfigMap.replace(serviceId, oldSettableFuture, settableFuture);
        } catch (Exception ex) {
          LOG.debug("Unable to deserialize the config for service {}. Got data {}", serviceId, result.getData());
          // Need to remove the future from the map since later calls will continue to use this future and will think
          // that there is an exception
          routeConfigMap.remove(serviceId, settableFuture);
          settableFuture.setException(ex);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        // If no node is present, then set an empty RouteConfig (instead of setting exception since generating
        // stack trace, when future.get is called, is expensive). Also place a watch on the node to monitor further
        // data updates. If its an exception other than NoNodeException, then remove the future, so that
        // zkClient#getData will be retried during the next fetch config request
        if (t instanceof KeeperException.NoNodeException) {
          settableFuture.set(new RouteConfig(Collections.<String, Integer>emptyMap()));
          existsAndWatch(serviceId, settableFuture);
        } else {
          settableFuture.setException(t);
          routeConfigMap.remove(serviceId, settableFuture);
        }
      }
    });
    return settableFuture;
  }

  private void existsAndWatch(final ProgramId serviceId, final SettableFuture<RouteConfig> oldSettableFuture) {
    Futures.addCallback(zkClient.exists(getZKPath(serviceId), new Watcher() {
      @Override
      public void process(WatchedEvent event) {
        // If service name doesn't exist in the map, then don't rewatch it.
        if (!routeConfigMap.containsKey(serviceId)) {
          return;
        }

        if (event.getType() == Event.EventType.NodeCreated) {
          getAndWatchData(serviceId, SettableFuture.<RouteConfig>create(),
                          oldSettableFuture, new ZKRouteWatcher(serviceId));
        }
      }
    }), new FutureCallback<Stat>() {
      @Override
      public void onSuccess(@Nullable Stat result) {
        if (result != null) {
          getAndWatchData(serviceId, SettableFuture.<RouteConfig>create(),
                          oldSettableFuture, new ZKRouteWatcher(serviceId));
        }
      }

      @Override
      public void onFailure(Throwable t) {
        routeConfigMap.remove(serviceId);
        LOG.debug("Failed to check exists for property data for {}", serviceId, t);
      }
    });
  }

  @Override
  public void close() throws Exception {
    // Clear the map, so that any active watches will expire.
    routeConfigMap.clear();
  }

  private class ZKRouteWatcher implements Watcher {
    private final ProgramId serviceId;

    ZKRouteWatcher(ProgramId serviceId) {
      this.serviceId = serviceId;
    }

    @Override
    public void process(WatchedEvent event) {
      // If service name doesn't exist in the map, then don't re-watch it.
      SettableFuture<RouteConfig> oldSettableFuture = routeConfigMap.get(serviceId);
      if (oldSettableFuture == null) {
        return;
      }

      if (event.getType() == Event.EventType.NodeDeleted) {
        // Remove the mapping from cache
        routeConfigMap.remove(serviceId, oldSettableFuture);
        return;
      }

      // Create a new settable future, since we don't want to set the existing future again
      getAndWatchData(serviceId, SettableFuture.<RouteConfig>create(), oldSettableFuture, this);
    }
  }
}
