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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.operations.OperationalStats;
import co.cask.cdap.operations.OperationalStatsUtils;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.gson.Gson;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * {@link co.cask.http.HttpHandler} for service provider statistics.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/system/serviceproviders")
public class OperationalStatsHttpHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(OperationalStatsHttpHandler.class);
  private static final Gson GSON = new Gson();

  @GET
  @Path("/")
  public void getServiceProviders(HttpRequest request, HttpResponder responder) throws Exception {
    // we want to fetch stats with the stat type 'info' grouped by the service name
    responder.sendJson(HttpResponseStatus.OK,
                       GSON.toJson(getStats(OperationalStatsUtils.STAT_TYPE_KEY, OperationalStatsUtils.STAT_TYPE_INFO,
                                            OperationalStatsUtils.SERVICE_NAME_KEY)));
  }

  @GET
  @Path("/{service-provider}/stats")
  public void getServiceProviderStats(HttpRequest request, HttpResponder responder,
                                      @PathParam("service-provider") String serviceProvider) throws Exception {
    // we want to fetch stats with the specified service name grouped by the stat type
    Map<String, Map<String, Object>> stats =
      getStats(OperationalStatsUtils.SERVICE_NAME_KEY, serviceProvider, OperationalStatsUtils.STAT_TYPE_KEY);
    if (stats.isEmpty()) {
      throw new NotFoundException(String.format("Service provider %s not found", serviceProvider));
    }
    // info is only needed in the list API, not in the stats API
    stats.remove(OperationalStatsUtils.STAT_TYPE_INFO);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(stats));
  }

  /**
   * Reads operational stats collected using the {@link OperationalStats} extension mechanism with the specified
   * property key and value, grouped by the specified groupByKey.
   *
   * @param propertyKey the key that must be contained with the specified value in the stat to be returned
   * @param propertyValue the value that the specified key must have in the stat to be returned
   * @param groupByKey an additional key in the stat's property to group the stats by
   * @return a {@link Map} of the group to a {@link Map} of stats of that group
   * @throws Exception when there are errors reading stats using JMX
   */
  @VisibleForTesting
  Map<String, Map<String, Object>> getStats(String propertyKey, String propertyValue,
                                            String groupByKey) throws Exception {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(propertyKey), "Property should not be null or empty.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(propertyValue), "Property value should not be null or empty.");
    Hashtable<String, String> properties = new Hashtable<>();
    // we want stats with the specified value for the specified key, so set them in the properties
    properties.put(propertyKey, propertyValue);
    // since we want to group by the groupKey, we want to fetch all groups
    properties.put(groupByKey, "*");
    ObjectName objectName = new ObjectName(OperationalStatsUtils.JMX_DOMAIN, properties);
    Map<String, Map<String, Object>> result = new HashMap<>();
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (ObjectName name : mbs.queryNames(objectName, null)) {
      String group = name.getKeyProperty(groupByKey);
      MBeanInfo mBeanInfo = mbs.getMBeanInfo(name);
      Map<String, Object> stats = new HashMap<>();
      for (MBeanAttributeInfo attributeInfo : mBeanInfo.getAttributes()) {
        stats.put(attributeInfo.getName(), mbs.getAttribute(name, attributeInfo.getName()));
      }
      result.put(group, stats);
      LOG.trace("Found stats of group {} as {}", group, stats);
    }
    return result;
  }
}
