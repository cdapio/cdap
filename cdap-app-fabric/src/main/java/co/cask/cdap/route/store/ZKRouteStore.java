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
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.internal.guava.reflect.TypeToken;
import co.cask.cdap.proto.id.ProgramId;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.zookeeper.NodeData;
import org.apache.twill.zookeeper.ZKClient;
import org.apache.twill.zookeeper.ZKOperations;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * RouteStore where the routes are stored in a ZK persistent node. This is intended for use in distributed mode.
 */
public class ZKRouteStore implements RouteStore {
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_INTEGER_TYPE = new TypeToken<Map<String, Integer>>() { }.getType();

  private final ZKClient zkClient;

  @Inject
  public ZKRouteStore(ZKClient zkClient) {
    this.zkClient = zkClient;
  }

  @Override
  public void store(ProgramId serviceId, RouteConfig routeConfig) {
    byte[] payload = Bytes.toBytes(GSON.toJson(routeConfig.getRoutes()));
    Futures.getUnchecked(
      ZKOperations.ignoreError(zkClient.create(getZKPath(serviceId), payload, CreateMode.PERSISTENT),
                               KeeperException.NodeExistsException.class, null));
  }

  @Override
  public void delete(final ProgramId serviceId) throws NotFoundException {
    try {
      zkClient.delete(getZKPath(serviceId)).get(5, TimeUnit.SECONDS);
    } catch (InterruptedException | TimeoutException e) {
      Throwables.propagate(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof KeeperException.NoNodeException) {
        throw new NotFoundException(String.format("Route Config for Service %s was not found.", serviceId));
      }
      Throwables.propagate(e);
    }
  }

  @Override
  public RouteConfig fetch(ProgramId serviceId) throws NotFoundException {
    try {
      zkClient.getData(getZKPath(serviceId)).get(5, TimeUnit.SECONDS);
      NodeData nodeData = Futures.getUnchecked(zkClient.getData(getZKPath(serviceId)));
      Map<String, Integer> routes = GSON.fromJson(Bytes.toString(nodeData.getData()), MAP_STRING_INTEGER_TYPE);
      return new RouteConfig(routes);
    } catch (InterruptedException | TimeoutException e) {
      Throwables.propagate(e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof KeeperException.NoNodeException) {
        throw new NotFoundException(String.format("Route Config for Service %s was not found.", serviceId));
      }
      Throwables.propagate(e);
    }
    return null;
  }

  private static String getZKPath(ProgramId serviceId) {
    return String.format("/routestore/%s", ServiceDiscoverable.getName(serviceId));
  }
}
