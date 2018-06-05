/*
 * Copyright © 2014-2018 Cask Data, Inc.
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
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.AbstractHttpHandler;
import io.netty.handler.codec.http.HttpRequest;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;

/**
 * Class to match the request path to corresponding service like app-fabric, or metrics service.
 */
public final class RouterPathLookup extends AbstractHttpHandler {

  @SuppressWarnings("unused")
  private enum AllowedMethod {
    GET, PUT, POST, DELETE
  }

  public static final RouteDestination APP_FABRIC_HTTP = new RouteDestination(Constants.Service.APP_FABRIC_HTTP);
  public static final RouteDestination METRICS = new RouteDestination(Constants.Service.METRICS);
  public static final RouteDestination DATASET_MANAGER = new RouteDestination(Constants.Service.DATASET_MANAGER);
  public static final RouteDestination METADATA_SERVICE = new RouteDestination(Constants.Service.METADATA_SERVICE);
  public static final RouteDestination EXPLORE_HTTP_USER_SERVICE = new RouteDestination(
    Constants.Service.EXPLORE_HTTP_USER_SERVICE);
  public static final RouteDestination STREAMS_SERVICE = new RouteDestination(Constants.Service.STREAMS);
  public static final RouteDestination PREVIEW_HTTP = new RouteDestination(Constants.Service.PREVIEW_HTTP);
  public static final RouteDestination TRANSACTION = new RouteDestination(Constants.Service.TRANSACTION_HTTP);
  public static final RouteDestination LOG_SAVER = new RouteDestination(Constants.Service.LOGSAVER);
  public static final RouteDestination METRICS_PROCESSOR = new RouteDestination(Constants.Service.METRICS_PROCESSOR);
  public static final RouteDestination DATASET_EXECUTOR = new RouteDestination(Constants.Service.DATASET_EXECUTOR);
  public static final RouteDestination MESSAGING = new RouteDestination(Constants.Service.MESSAGING_SERVICE);
  public static final RouteDestination DONT_ROUTE = new RouteDestination(Constants.Router.DONT_ROUTE_SERVICE);

  /**
   * Returns the CDAP service which will handle the HttpRequest
   *
   * @param requestPath Normalized (and query string removed) URI path
   * @param httpRequest HttpRequest used to get the Http method and account id
   * @return destination service
   */
  @Nullable
  public RouteDestination getRoutingService(String requestPath, HttpRequest httpRequest) {
    try {
      String method = httpRequest.method().name();
      AllowedMethod requestMethod = AllowedMethod.valueOf(method);
      String[] uriParts = StringUtils.split(requestPath, '/');

      if (uriParts[0].equals(Constants.Gateway.API_VERSION_3_TOKEN)) {
        return getV3RoutingService(uriParts, requestMethod);
      }
    } catch (Exception e) {
      // Ignore exception. Default routing to app-fabric.
    }
    return APP_FABRIC_HTTP;
  }

  private boolean isUserServiceType(String uriPart) {
    for (ProgramType type : ServiceDiscoverable.getUserServiceTypes()) {
      if (type.getCategoryName().equals(uriPart)) {
        return true;
      }
    }
    return false;
  }

  @Nullable
  private RouteDestination getV3RoutingService(String [] uriParts, AllowedMethod requestMethod) {
    if ((uriParts.length >= 2) && uriParts[1].equals("feeds")) {
      // TODO find a better way to handle that - this looks hackish
      return null;
    } else if ("dashboard".equals(uriParts[1])) {
      return APP_FABRIC_HTTP;
    } else if ((uriParts.length >= 11) && "versions".equals(uriParts[5]) && isUserServiceType(uriParts[7])
      && "methods".equals(uriParts[9])) {
      // User defined services (version specific) handle methods on them:
      //Path: "/v3/namespaces/{namespace-id}/apps/{app-id}/versions/{version-id}/services/{service-id}/methods/
      //       <user-defined-method-path>"
      String serviceName = ServiceDiscoverable.getName(uriParts[2], uriParts[4],
                                                       ProgramType.valueOfCategoryName(uriParts[7]), uriParts[8]);
      String version = uriParts[6];
      return new RouteDestination(serviceName, version);
    } else if ((uriParts.length >= 9) && isUserServiceType(uriParts[5]) && "methods".equals(uriParts[7])) {
      //User defined services handle methods on them:
      //Path: "/v3/namespaces/{namespace-id}/apps/{app-id}/services/{service-id}/methods/<user-defined-method-path>"
      return new RouteDestination(ServiceDiscoverable.getName(uriParts[2], uriParts[4],
                                                              ProgramType.valueOfCategoryName(uriParts[5]),
                                                              uriParts[6]));
    } else if (matches(uriParts, "v3", "system", "services", null, "logs")) {
      //Log Handler Path /v3/system/services/<service-id>/logs
      return METRICS;
    } else if (matches(uriParts, "v3", "namespaces", null, "apps", null, "metadata") ||
      matches(uriParts, "v3", "namespaces", null, "apps", null, null, null, "metadata") ||
      matches(uriParts, "v3", "namespaces", null, "artifacts", null, "versions", null, "metadata") ||
      matches(uriParts, "v3", "namespaces", null, "datasets", null, "metadata") ||
      matches(uriParts, "v3", "namespaces", null, "streams", null, "metadata") ||
      matches(uriParts, "v3", "namespaces", null, "streams", null, "views", null, "metadata") ||

      matches(uriParts, "v3", "namespaces", null, "apps", null, "metadata", "properties") ||
      matches(uriParts, "v3", "namespaces", null, "artifacts", null, "versions", null, "metadata", "properties") ||
      matches(uriParts, "v3", "namespaces", null, "apps", null, null, null, "metadata", "properties") ||
      matches(uriParts, "v3", "namespaces", null, "datasets", null, "metadata", "properties") ||
      matches(uriParts, "v3", "namespaces", null, "streams", null, "metadata", "properties") ||
      matches(uriParts, "v3", "namespaces", null, "streams", null, "views", null, "metadata", "properties") ||

      matches(uriParts, "v3", "namespaces", null, "apps", null, "metadata", "tags") ||
      matches(uriParts, "v3", "namespaces", null, "artifacts", null, "versions", null, "metadata", "tags") ||
      matches(uriParts, "v3", "namespaces", null, "apps", null, null, null, "metadata", "tags") ||
      matches(uriParts, "v3", "namespaces", null, "datasets", null, "metadata", "tags") ||
      matches(uriParts, "v3", "namespaces", null, "streams", null, "metadata", "tags") ||
      matches(uriParts, "v3", "namespaces", null, "streams", null, "views", null, "metadata", "tags") ||

      matches(uriParts, "v3", "namespaces", null, "metadata", "search") ||
      matches(uriParts, "v3", "namespaces", null, "datasets", null, "lineage") ||
      matches(uriParts, "v3", "namespaces", null, "streams", null, "lineage") ||
      matches(uriParts, "v3", "namespaces", null, "apps", null, null, null, "runs", null, "metadata")) {
      return METADATA_SERVICE;
    } else if (matches(uriParts, "v3", "security", "authorization") ||
      matches(uriParts, "v3", "namespaces", null, "securekeys")) {
      // Authorization and Secure Store Handlers currently run in App Fabric
      return APP_FABRIC_HTTP;
    } else if (matches(uriParts, "v3", "security", "store", "namespaces", null)) {
      return APP_FABRIC_HTTP;
    } else if ((matches(uriParts, "v3", "namespaces", null, "streams", null, "programs")
      || matches(uriParts, "v3", "namespaces", null, "data", "datasets", null, "programs")) &&
      requestMethod.equals(AllowedMethod.GET)) {
      return APP_FABRIC_HTTP;
    } else if (matches(uriParts, "v3", "namespaces", null, "previews")) {
      return PREVIEW_HTTP;
    } else if (matches(uriParts, "v3", "system", "serviceproviders")) {
      return APP_FABRIC_HTTP;
    } else if ((uriParts.length >= 4) && uriParts[1].equals("namespaces") && uriParts[3].equals("streams")) {
      return STREAMS_SERVICE;
    } else if ((uriParts.length >= 8 && uriParts[7].equals("logs")) ||
      (uriParts.length >= 10 && uriParts[9].equals("logs")) ||
      (uriParts.length >= 6 && uriParts[5].equals("logs"))) {
      //Log Handler Paths:
      // /v3/namespaces/<namespaceid>/apps/<appid>/<programid-type>/<programid>/logs
      // /v3/namespaces/{namespace-id}/apps/{app-id}/{program-type}/{program-id}/runs/{run-id}/logs
      return METRICS;
    } else if (uriParts.length >= 2 && uriParts[1].equals("metrics")) {
      //Metrics Search Handler Path /v3/metrics
      return METRICS;
    } else if (uriParts.length >= 5 && uriParts[1].equals("data") && uriParts[2].equals("explore") &&
      (uriParts[3].equals("queries") || uriParts[3].equals("jdbc") || uriParts[3].equals("namespaces"))) {
      // non-namespaced explore operations. For example, /v3/data/explore/queries/{id}
      return EXPLORE_HTTP_USER_SERVICE;
    } else if (uriParts.length >= 6 && uriParts[3].equals("data") && uriParts[4].equals("explore") &&
      (uriParts[5].equals("queries") || uriParts[5].equals("streams") || uriParts[5].equals("datasets")
        || uriParts[5].equals("tables") || uriParts[5].equals("jdbc"))) {
      // namespaced explore operations. For example, /v3/namespaces/{namespace-id}/data/explore/streams/{stream}/enable
      return EXPLORE_HTTP_USER_SERVICE;
    } else if ((uriParts.length == 3) && uriParts[1].equals("explore") && uriParts[2].equals("status")) {
      return EXPLORE_HTTP_USER_SERVICE;
    } else if (matches(uriParts, "v3", "system", "services", null, "status")
      || matches(uriParts, "v3", "system", "services", null, "stacks")) {
      switch (uriParts[3]) {
        case Constants.Service.LOGSAVER: return LOG_SAVER;
        case Constants.Service.TRANSACTION: return TRANSACTION;
        case Constants.Service.METRICS_PROCESSOR: return METRICS_PROCESSOR;
        case Constants.Service.METRICS: return METRICS;
        case Constants.Service.APP_FABRIC_HTTP: return APP_FABRIC_HTTP;
        case Constants.Service.STREAMS: return STREAMS_SERVICE;
        case Constants.Service.DATASET_EXECUTOR: return DATASET_EXECUTOR;
        case Constants.Service.METADATA_SERVICE: return METADATA_SERVICE;
        case Constants.Service.EXPLORE_HTTP_USER_SERVICE: return EXPLORE_HTTP_USER_SERVICE;
        case Constants.Service.MESSAGING_SERVICE: return MESSAGING;
        default: return null;
      }
    } else if (uriParts.length == 7 && uriParts[3].equals("data") && uriParts[4].equals("datasets") &&
      (uriParts[6].equals("flows") || uriParts[6].equals("workers") || uriParts[6].equals("mapreduce"))) {
      // namespaced app fabric data operations:
      // /v3/namespaces/{namespace-id}/data/datasets/{name}/flows
      // /v3/namespaces/{namespace-id}/data/datasets/{name}/workers
      // /v3/namespaces/{namespace-id}/data/datasets/{name}/mapreduce
      return APP_FABRIC_HTTP;
    } else if ((uriParts.length >= 4) && uriParts[3].equals("data")) {
      // other data operations. For example:
      // /v3/namespaces/{namespace-id}/data/datasets
      // /v3/namespaces/{namespace-id}/data/datasets/{name}
      // /v3/namespaces/{namespace-id}/data/datasets/{name}/properties
      // /v3/namespaces/{namespace-id}/data/datasets/{name}/admin/{method}
      return DATASET_MANAGER;
    } else if ((uriParts.length >= 4) && uriParts[3].equals("artifact-internals")) {
      // we don't want to expose endpoints in artifact handler that are internal and can only by called by programs
      // /v3/namespaces/{namespace-id}/artifact-internals/list/artifacts
      // /v3/namespaces/{namespace-id}/artifact-internals/artifact/{artifact-name}
      return DONT_ROUTE;
    }
    return APP_FABRIC_HTTP;
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
}
