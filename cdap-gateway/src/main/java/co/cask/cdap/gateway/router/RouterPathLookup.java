/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data.stream.service.StreamHandler;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetInstanceHandler;
import co.cask.cdap.data2.datafabric.dataset.service.DatasetTypeHandler;
import co.cask.cdap.explore.executor.ExploreExecutorHttpHandler;
import co.cask.cdap.explore.executor.ExploreMetadataHttpHandler;
import co.cask.cdap.explore.executor.ExploreStatusHandler;
import co.cask.cdap.explore.executor.NamespacedExploreMetadataHttpHandler;
import co.cask.cdap.explore.executor.NamespacedQueryExecutorHttpHandler;
import co.cask.cdap.explore.executor.QueryExecutorHttpHandler;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AdapterHttpHandler;
import co.cask.cdap.gateway.handlers.AppFabricDataHttpHandler;
import co.cask.cdap.gateway.handlers.AppLifecycleHttpHandler;
import co.cask.cdap.gateway.handlers.ApplicationTemplateHandler;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import co.cask.cdap.gateway.handlers.ConfigHandler;
import co.cask.cdap.gateway.handlers.ConsoleSettingsHttpHandler;
import co.cask.cdap.gateway.handlers.DashboardHttpHandler;
import co.cask.cdap.gateway.handlers.MonitorHandler;
import co.cask.cdap.gateway.handlers.NamespaceHttpHandler;
import co.cask.cdap.gateway.handlers.NotificationFeedHttpHandler;
import co.cask.cdap.gateway.handlers.PreferencesHttpHandler;
import co.cask.cdap.gateway.handlers.ProgramLifecycleHttpHandler;
import co.cask.cdap.gateway.handlers.TransactionHttpHandler;
import co.cask.cdap.gateway.handlers.UsageHandler;
import co.cask.cdap.gateway.handlers.VersionHandler;
import co.cask.cdap.gateway.handlers.WorkflowHttpHandler;
import co.cask.cdap.metrics.query.MetricsHandler;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * Class to match the request path to corresponding service like app-fabric, or metrics service.
 */
public final class RouterPathLookup extends AuthenticatedHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RouterPathLookup.class);

  private static final Map<String, ? extends List<Class<?>>> HANDLERS = ImmutableMap.of(
    Constants.Service.STREAMS, ImmutableList.<Class<?>>of(
      StreamHandler.class),
    Constants.Service.METRICS, ImmutableList.<Class<?>>of(
      MetricsHandler.class),
    Constants.Service.APP_FABRIC_HTTP, ImmutableList.<Class<?>>of(
      AppFabricDataHttpHandler.class,
      AppLifecycleHttpHandler.class,
      ProgramLifecycleHttpHandler.class,
      ConfigHandler.class,
      VersionHandler.class,
      MonitorHandler.class,
      UsageHandler.class,
      NamespaceHttpHandler.class,
      NotificationFeedHttpHandler.class,
      DashboardHttpHandler.class,
      PreferencesHttpHandler.class,
      ConsoleSettingsHttpHandler.class,
      TransactionHttpHandler.class,
      AdapterHttpHandler.class,
      ApplicationTemplateHandler.class,
      WorkflowHttpHandler.class),
    Constants.Service.DATASET_MANAGER, ImmutableList.<Class<?>>of(
      DatasetInstanceHandler.class,
      DatasetTypeHandler.class),
    Constants.Service.EXPLORE_HTTP_USER_SERVICE, ImmutableList.<Class<?>>of(
      ExploreExecutorHttpHandler.class,
      ExploreMetadataHttpHandler.class,
      ExploreStatusHandler.class,
      NamespacedExploreMetadataHttpHandler.class,
      NamespacedQueryExecutorHttpHandler.class,
      QueryExecutorHttpHandler.class)
  );

  private MatchTree matcher;

  @Inject
  public RouterPathLookup(Authenticator authenticator) throws ConflictingRouteException {
    super(authenticator);
    matcher = createMatchTree(HANDLERS);
  }

  @SuppressWarnings("unused")
  private enum AllowedMethod {
    GET, PUT, POST, DELETE
  }

  /**
   * Returns the CDAP service which will handle the HttpRequest
   *
   * @param fallbackService service to which we fall back to if we can't determine the destination from the URI path
   * @param requestPath Normalized (and query string removed) URI path
   * @param httpRequest HttpRequest used to get the Http method and account id
   * @return destination service
   */
  public String getRoutingService(String fallbackService, String requestPath, HttpRequest httpRequest) {
    try {
      String method = httpRequest.getMethod().getName();
      AllowedMethod requestMethod = AllowedMethod.valueOf(method);
      String[] uriParts = StringUtils.split(requestPath, '/');

      //Check if the call should go to webapp
      //If service contains "$HOST" and if first split element is NOT the gateway version, then send it to WebApp
      //WebApp serves only static files (HTML, CSS, JS) and so /<appname> calls should go to WebApp
      //But stream calls issued by the UI should be routed to the appropriate CDAP service
      boolean isV3request = requestPath.startsWith(Constants.Gateway.API_VERSION_3);
      if (!isV3request && fallbackService.contains("$HOST")) {
        return fallbackService;
      } else if (isV3request) {
        return matcher.match(httpRequest.getMethod(), requestPath);
        // return getV3RoutingService(uriParts, requestMethod);
      }
    } catch (Exception e) {
      // Ignore exception. Default routing to app-fabric.
    }
    return Constants.Service.APP_FABRIC_HTTP;
  }

  private String getV3RoutingService(String [] uriParts, AllowedMethod requestMethod) {
    if ((uriParts.length >= 2) && uriParts[1].equals("feeds")) {
      // TODO find a better way to handle that - this looks hackish
      return null;
    } else if ((uriParts.length >= 9) && "services".equals(uriParts[5]) && "methods".equals(uriParts[7])) {
      //User defined services handle methods on them:
      //Path: "/v3/namespaces/{namespace-id}/apps/{app-id}/services/{service-id}/methods/<user-defined-method-path>"
      //Discoverable Service Name -> "service.%s.%s.%s", namespaceId, appId, serviceId
      return String.format("service.%s.%s.%s", uriParts[2], uriParts[4], uriParts[6]);
    } else if (matches(uriParts, "v3", "system", "services", null, "logs")) {
      //Log Handler Path /v3/system/services/<service-id>/logs
      return Constants.Service.METRICS;
    } else if ((matches(uriParts, "v3", "namespaces", null, "streams", null, "adapters")
      || matches(uriParts, "v3", "namespaces", null, "streams", null, "programs")
      || matches(uriParts, "v3", "namespaces", null, "data", "datasets", null, "adapters")
      || matches(uriParts, "v3", "namespaces", null, "data", "datasets", null, "programs")) &&
      requestMethod.equals(AllowedMethod.GET)) {
      return Constants.Service.APP_FABRIC_HTTP;
    } else if ((uriParts.length >= 4) && uriParts[1].equals("namespaces") && uriParts[3].equals("streams")) {
      // /v3/namespaces/<namespace>/streams goes to AppFabricHttp
      // All else go to Stream Handler
      if (uriParts.length == 4) {
        return Constants.Service.APP_FABRIC_HTTP;
      } else {
        return Constants.Service.STREAMS;
      }
    } else if ((uriParts.length >= 8 && uriParts[7].equals("logs")) ||
      (uriParts.length >= 10 && uriParts[9].equals("logs")) ||
      (uriParts.length >= 6 && uriParts[5].equals("logs"))) {
      //Log Handler Paths:
      // /v3/namespaces/<namespaceid>/apps/<appid>/<programid-type>/<programid>/logs
      // /v3/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/runs/{run-id}/logs
      // /v3/namespaces/<namespaceid>/adapters/<adapterid>/logs
      // /v3/namespaces/{namespace-id}/adapters/{adapter-id}/runs/{run-id}/logs (same as case 1)
      return Constants.Service.METRICS;
    } else if (uriParts.length >= 2 && uriParts[1].equals("metrics")) {
      //Metrics Search Handler Path /v3/metrics
      return Constants.Service.METRICS;
    } else if (uriParts.length >= 5 && uriParts[1].equals("data") && uriParts[2].equals("explore") &&
      (uriParts[3].equals("queries") || uriParts[3].equals("jdbc") || uriParts[3].equals("namespaces"))) {
      // non-namespaced explore operations. For example, /v3/data/explore/queries/{id}
      return Constants.Service.EXPLORE_HTTP_USER_SERVICE;
    } else if (uriParts.length >= 6 && uriParts[3].equals("data") && uriParts[4].equals("explore") &&
      (uriParts[5].equals("queries") || uriParts[5].equals("streams") || uriParts[5].equals("datasets")
        || uriParts[5].equals("tables") || uriParts[5].equals("jdbc"))) {
      // namespaced explore operations. For example, /v3/namespaces/{namespace-id}/data/explore/streams/{stream}/enable
      return Constants.Service.EXPLORE_HTTP_USER_SERVICE;
    } else if ((uriParts.length == 3) && uriParts[1].equals("explore") && uriParts[2].equals("status")) {
      return Constants.Service.EXPLORE_HTTP_USER_SERVICE;
    } else if (uriParts.length == 7 && uriParts[3].equals("data") && uriParts[4].equals("datasets") &&
      (uriParts[6].equals("flows") || uriParts[6].equals("workers") || uriParts[6].equals("mapreduce"))) {
      // namespaced app fabric data operations:
      // /v3/namespaces/{namespace-id}/data/datasets/{name}/flows
      // /v3/namespaces/{namespace-id}/data/datasets/{name}/workers
      // /v3/namespaces/{namespace-id}/data/datasets/{name}/mapreduce
      return Constants.Service.APP_FABRIC_HTTP;
    } else if ((uriParts.length >= 4) && uriParts[3].equals("data")) {
      // other data operations. For example:
      // /v3/namespaces/{namespace-id}/data/datasets
      // /v3/namespaces/{namespace-id}/data/datasets/{name}
      // /v3/namespaces/{namespace-id}/data/datasets/{name}/properties
      // /v3/namespaces/{namespace-id}/data/datasets/{name}/admin/{method}
      return Constants.Service.DATASET_MANAGER;
    }
    return Constants.Service.APP_FABRIC_HTTP;
  }

  /**
   * Determines if actual matches expected.
   *
   * - actual may be longer than expected, but we'll return true as long as expected was found
   * - null in expected means "accept any string"
   *
   * @param actual actual string array to check
   * @param expected expected string array format
   * @return true if actual matches expected
   */
  private boolean matches(String[] actual, String... expected) {
    if (actual.length < expected.length) {
      return false;
    }

    for (int i = 0; i < expected.length; i++) {
      if (expected[i] == null) {
        continue;
      }
      if (!expected[i].equals(actual[i])) {
        return false;
      }
    }
    return true;
  }

  private static MatchTree createMatchTree(Map<String, ? extends List<Class<?>>> serviceMap)
    throws ConflictingRouteException {

    // start with an empty decision tree
    MatchTree matchTree = new MatchTree();

    // add NO_ROUTE for feeds
    matchTree.addRoute(null, "/v3/feeds", MatchTree.NO_ROUTE);

    // add all methods of all handlers
    for (String service : serviceMap.keySet()) {
      for (Class<?> handlerClass : serviceMap.get(service)) {
        Path handlerPathAnnotation = handlerClass.getAnnotation(Path.class);
        String handlerPath = handlerPathAnnotation == null ? "" : handlerPathAnnotation.value();
        for (Method method : handlerClass.getMethods()) {
          Path methodPathAnnotation = method.getAnnotation(Path.class);
          if (methodPathAnnotation != null) {
            String methodPath = methodPathAnnotation.value();
            String completePath = handlerPath.endsWith("/") || methodPath.startsWith("/")
              ? handlerPath + methodPath : handlerPath + "/" + methodPath;
            HttpMethod httpMethod;
            if (method.isAnnotationPresent(GET.class)) {
              httpMethod = HttpMethod.GET;
            } else if (method.isAnnotationPresent(PUT.class)) {
              httpMethod = HttpMethod.PUT;
            } else if (method.isAnnotationPresent(POST.class)) {
              httpMethod = HttpMethod.POST;
            } else if (method.isAnnotationPresent(DELETE.class)) {
              httpMethod = HttpMethod.DELETE;
            } else {
              LOG.warn("Ignoring method {} with @Path '{}' in handler {} that has no HTTP method annotation",
                       method.getName(), methodPath, handlerClass.getName());
              continue;
            }
            LOG.debug("Adding {} '{}' for handler {}", httpMethod, completePath, handlerClass.getName());
            try {
              matchTree.addRoute(httpMethod, completePath, service);
            } catch (ConflictingRouteException e) {
              LOG.error("Error adding {} '{}' for handler {}: {}",
                        httpMethod, completePath, handlerClass.getName(), e.getMessage());
              throw e;
            }
          }
        }
      }
    }
    String userServiceMethodPath = "/v3/namespaces/{...}/apps/{...}/services/{...}/methods/{...}";
    LOG.debug("Adding route '{}' for user service handler", userServiceMethodPath);
    try {
      matchTree.addRoute(null, userServiceMethodPath, MatchTree.USER_SERVICE_FORMAT);
    } catch (ConflictingRouteException e) {
      LOG.error("Error adding route '{}' for user service handler: {}", userServiceMethodPath, e.getMessage());
      throw e;
    }
    matchTree.optimize();
    return matchTree;
  }


  public static void main(String[] args) throws ConflictingRouteException {
    MatchTree root = createMatchTree(HANDLERS);
    System.out.println(root);
  }

}
