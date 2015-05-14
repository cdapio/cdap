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
import co.cask.cdap.data.stream.service.StreamFetchHandler;
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
import co.cask.cdap.logging.gateway.handlers.LogHandler;
import co.cask.cdap.metrics.query.MetricsHandler;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
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

  public static final String USER_SERVICE_FORMAT = "service.%s.%s.%s";

  private static final Logger LOG = LoggerFactory.getLogger(RouterPathLookup.class);
  private static final String USER_SERVICE_METHOD_PATH = "/v3/namespaces/{...}/apps/{...}/services/{...}/methods/{...}";
  private static final String NO_ROUTE = "";

  private static final Map<String, ? extends List<Class<?>>> HANDLERS = ImmutableMap.of(
    Constants.Service.STREAMS, ImmutableList.<Class<?>>of(
      StreamHandler.class,
      StreamFetchHandler.class),
    Constants.Service.METRICS, ImmutableList.<Class<?>>of(
      MetricsHandler.class,
      LogHandler.class),
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

  private PrefixTree matcher;

  @Inject
  public RouterPathLookup(Authenticator authenticator) throws ConflictingRouteException {
    super(authenticator);
    matcher = createMatcher(HANDLERS);
    LOG.trace("Router matcher is: {}", matcher);
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

      //Check if the call should go to webapp
      //If service contains "$HOST" and if first split element is NOT the gateway version, then send it to WebApp
      //WebApp serves only static files (HTML, CSS, JS) and so /<appname> calls should go to WebApp
      //But stream calls issued by the UI should be routed to the appropriate CDAP service
      int pos = 0;
      while (pos < requestPath.length() && requestPath.charAt(pos) == '/') {
        pos++;
      }
      boolean isV3request = requestPath.startsWith(Constants.Gateway.API_VERSION_3_TOKEN, pos);
      if (!isV3request && fallbackService.contains("$HOST")) {
        return fallbackService;
      } else if (isV3request) {
        String service = matcher.match(requestPath);
        if (service != null) {
          if (NO_ROUTE.equals(service)) {
            service = null;
          }
          return service;
        }
      }
    } catch (Exception e) {
      // Ignore exception. Default routing to app-fabric.
    }
    return Constants.Service.APP_FABRIC_HTTP;
  }

  private static PrefixTree createMatcher(Map<String, ? extends List<Class<?>>> serviceMap)
    throws ConflictingRouteException {

    // start with an empty decision tree
    PrefixTree prefixTree = new PrefixTree();

    // add NO_ROUTE for feeds
    prefixTree.addRoute("/v3/feeds", NO_ROUTE, new PrefixTree.Route("no-handler", "no-method", "ALL", "/v3/feeds"));

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
              prefixTree.addRoute(completePath, service, new PrefixTree.Route(
                handlerClass.getSimpleName(), method.getName(), httpMethod, completePath));
            } catch (ConflictingRouteException e) {
              LOG.error("Error adding {} '{}' for handler {}: {}",
                        httpMethod, completePath, handlerClass.getName(), e.getMessage());
              throw e;
            }
          }
        }
      }
    }
    LOG.debug("Adding route '{}' for user service handler", USER_SERVICE_METHOD_PATH);
    try {
      prefixTree.addRoute(USER_SERVICE_METHOD_PATH, USER_SERVICE_FORMAT,
                          new PrefixTree.Route("user-service", "user-method", "ALL", USER_SERVICE_METHOD_PATH));
    } catch (ConflictingRouteException e) {
      LOG.error("Error adding route '{}' for user service handler: {}", USER_SERVICE_METHOD_PATH, e.getMessage());
      throw e;
    }
    prefixTree.optimize();
    return prefixTree;
  }

  // this main method is here to help debugging. It prints the entire prefix tree with all information.
  public static void main(String[] args) throws ConflictingRouteException {
    PrefixTree root = createMatcher(HANDLERS);
    System.out.println(root.toString(true));
  }

}
