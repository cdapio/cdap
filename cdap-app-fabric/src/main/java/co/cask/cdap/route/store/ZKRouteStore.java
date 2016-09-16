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
import co.cask.cdap.internal.guava.reflect.TypeToken;
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
import org.apache.twill.common.Threads;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.OperationFuture;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * RouteStore where the routes are stored in a ZK persistent node. This is intended for use in distributed mode.
 */
public class ZKRouteStore implements RouteStore {
  private static final Logger LOG = LoggerFactory.getLogger(ZKRouteStore.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_INTEGER_TYPE = new TypeToken<Map<String, Integer>>() { }.getType();
  private static final int ZK_TIMEOUT_SECS = 5;

  private final ZKClient zkClient;
  private final ConcurrentMap<ProgramId, RouteConfig> routeConfigMap;

  @Inject
  public ZKRouteStore(ZKClient zkClient) {
    this.zkClient = zkClient;
    this.routeConfigMap = Maps.newConcurrentMap();
  }

  @Override
  public void store(final ProgramId serviceId, final RouteConfig routeConfig) {
    Supplier<RouteConfig> supplier = Suppliers.ofInstance(routeConfig);
    final SettableFuture<RouteConfig> settableFuture = SettableFuture.create();
    Futures.addCallback(ZKExtOperations.createOrSet(zkClient, getZKPath(serviceId), supplier, ROUTE_CONFIG_CODEC, 10),
                        new FutureCallback<RouteConfig>() {
                          @Override
                          public void onSuccess(RouteConfig result) {
                            routeConfigMap.put(serviceId, routeConfig);
                            settableFuture.set(result);
                          }

                          @Override
                          public void onFailure(Throwable t) {
                            settableFuture.setException(t);
                          }
                        });
    try {
      settableFuture.get(ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException ex) {
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public void delete(final ProgramId serviceId) throws NotFoundException {
    OperationFuture<String> future = zkClient.delete(getZKPath(serviceId));
    final SettableFuture<String> settableFuture = SettableFuture.create();
    Futures.addCallback(future, new FutureCallback<String>() {
      @Override
      public void onSuccess(String result) {
        routeConfigMap.remove(serviceId);
        settableFuture.set(result);
      }

      @Override
      public void onFailure(Throwable t) {
        settableFuture.setException(t);
      }
    }, Threads.SAME_THREAD_EXECUTOR);

    try {
      settableFuture.get(ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (ExecutionException | InterruptedException | TimeoutException ex) {
      if (ex.getCause() instanceof KeeperException.NoNodeException) {
        throw new NotFoundException(String.format("Route Config for Service %s was not found.", serviceId));
      }
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public RouteConfig fetch(final ProgramId serviceId) throws NotFoundException {
    if (routeConfigMap.containsKey(serviceId)) {
      RouteConfig routeConfig = routeConfigMap.get(serviceId);
      // Key is present but the value is null. So we have cached that this key has no config as of now. We have placed
      // a watch on it, so it will update it with the config if data is updated for that node.
      if (routeConfig == null) {
        throw new NotFoundException(String.format("Route Config for Service %s was not found.", serviceId));
      }
      return routeConfig;
    }

    Future<RouteConfig> settableFuture = getAndWatchData(serviceId, new ZKRouteWatcher(serviceId));
    try {
      return settableFuture.get(ZK_TIMEOUT_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      if (e.getCause() instanceof KeeperException.NoNodeException) {
        throw new NotFoundException(String.format("Route Config for Service %s was not found.", serviceId));
      }
      throw Throwables.propagate(e);
    }
  }

  private static String getZKPath(ProgramId serviceId) {
    return String.format("/routestore/%s", ServiceDiscoverable.getName(serviceId));
  }

  static final Codec<RouteConfig> ROUTE_CONFIG_CODEC = new Codec<RouteConfig>() {

    @Override
    public byte[] encode(RouteConfig object) throws IOException {
      return Bytes.toBytes(GSON.toJson(object.getRoutes()));
    }

    @Override
    public RouteConfig decode(byte[] data) throws IOException {
      Map<String, Integer> routes = GSON.fromJson(Bytes.toString(data), MAP_STRING_INTEGER_TYPE);
      return new RouteConfig(routes);
    }
  };

  private Future<RouteConfig> getAndWatchData(final ProgramId serviceId, Watcher watcher) {
    OperationFuture<NodeData> future = zkClient.getData(getZKPath(serviceId), watcher);
    final SettableFuture<RouteConfig> settableFuture = SettableFuture.create();
    Futures.addCallback(future, new FutureCallback<NodeData>() {
      @Override
      public void onSuccess(NodeData result) {
        try {
          RouteConfig route = ROUTE_CONFIG_CODEC.decode(result.getData());
          routeConfigMap.put(serviceId, route);
          settableFuture.set(route);
        } catch (IOException ex) {
          LOG.debug("Unable to deserialize the config for service {}. Got data {}", serviceId, result.getData());
          settableFuture.setException(ex);
        }
      }

      @Override
      public void onFailure(Throwable t) {
        if (t instanceof KeeperException.NoNodeException) {
          // If node doesn't exist, add that info to the map.
          routeConfigMap.put(serviceId, null);
        }
        settableFuture.setException(t);
      }
    });
    return settableFuture;
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
      if (!routeConfigMap.containsKey(serviceId)) {
        return;
      }

      if (event.getType() == Event.EventType.NodeDeleted) {
        // Remove the mapping from cache
        routeConfigMap.remove(serviceId);
        return;
      }
      getAndWatchData(serviceId, this);
    }
  }
}
