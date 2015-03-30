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
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.cdap.gateway.handlers.AuthenticatedHttpHandler;
import com.google.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * Class to match the request path to corresponding service like app-fabric, or metrics service.
 */
public final class RouterPathLookup extends AuthenticatedHttpHandler {

  @Inject
  public RouterPathLookup(Authenticator authenticator) {
    super(authenticator);
  }

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
      //But procedure/stream calls issued by the UI should be routed to the appropriate CDAP service
      if (fallbackService.contains("$HOST") && (uriParts.length >= 1)
        && (!(("/" + uriParts[0]).equals(Constants.Gateway.API_VERSION_2)))) {
        return fallbackService;
      }

      //TODO: The following code for now makes it possible to lookup services for /v3 paths.
      //This will have to be completely refactored once we know how all v3 paths look like (with namespaces)
      //We will have to take into consideration common patterns between v2 and v3 paths when we do refactor
      if (uriParts[0].equals(Constants.Gateway.API_VERSION_2_TOKEN)) {
        return getV2RoutingService(uriParts, requestMethod, httpRequest);
      } else if (uriParts[0].equals(Constants.Gateway.API_VERSION_3_TOKEN)) {
        return getV3RoutingService(uriParts, requestMethod, httpRequest);
      }
    } catch (Exception e) {
      // Ignore exception. Default routing to app-fabric.
    }
    return Constants.Service.APP_FABRIC_HTTP;
  }

  private String getV2RoutingService(String [] uriParts, AllowedMethod requestMethod, HttpRequest request) {
    if ((uriParts.length >= 2) && uriParts[1].equals("acls")) {
      return Constants.Service.ACL;
    } else if ((uriParts.length >= 2) && uriParts[1].equals("metrics")) {
      return Constants.Service.METRICS;
    } else if ((uriParts.length >= 2) && uriParts[1].equals("data")) {
      if ((uriParts.length >= 3) && uriParts[2].equals("explore")
        && (uriParts[3].equals("queries") || uriParts[3].equals("jdbc") || uriParts[3].equals("tables"))) {
        return Constants.Service.EXPLORE_HTTP_USER_SERVICE;
      } else if ((uriParts.length == 6) && uriParts[2].equals("explore") && uriParts[3].equals("datasets")) {
        // v2/data/explore/datasets/<dataset>/schema
        return Constants.Service.EXPLORE_HTTP_USER_SERVICE;
      }
      return Constants.Service.DATASET_MANAGER;
    } else if ((uriParts.length == 3) && uriParts[1].equals("explore") && uriParts[2].equals("status")) {
      return Constants.Service.EXPLORE_HTTP_USER_SERVICE;
    } else if ((uriParts.length >= 2) && uriParts[1].equals("streams")) {
      // /v2/streams should go to AppFabricHttp
      // /v2/streams/<stream-id> GET should go to AppFabricHttp, PUT, POST should go to Stream Handler
      // GET /v2/streams/flows should go to AppFabricHttp, rest should go Stream Handler
      if (uriParts.length == 2) {
        return Constants.Service.APP_FABRIC_HTTP;
      } else if (uriParts.length == 3) {
        return (requestMethod.equals(AllowedMethod.GET)) ?
          Constants.Service.APP_FABRIC_HTTP : Constants.Service.STREAMS;
      } else if ((uriParts.length == 4) && uriParts[3].equals("flows") && requestMethod.equals(AllowedMethod.GET)) {
        return Constants.Service.APP_FABRIC_HTTP;
      } else {
        return Constants.Service.STREAMS;
      }
    } else if ((uriParts.length >= 6) && uriParts[5].equals("logs")) {
      //Log Handler Path /v2/apps/<appid>/<programid-type>/<programid>/logs
      return Constants.Service.METRICS;
    } else if ((uriParts.length >= 5) && uriParts[4].equals("logs")) {
      //Log Handler Path /v2/system/services/<service-id>/logs
      return Constants.Service.METRICS;
    } else if ((uriParts.length >= 7) && uriParts[3].equals("procedures") && uriParts[5].equals("methods")) {
      //Procedure Path /v2/apps/<appid>/procedures/<procedureid>/methods/<methodName>
      //Discoverable Service Name -> procedure.%s.%s.%s", accountId, appId, procedureName ;
      String serviceName = String.format("procedure.%s.%s.%s", getAuthenticatedAccountId(request), uriParts[2],
                                         uriParts[4]);
      return serviceName;
    } else if ((uriParts.length >= 7) && uriParts[3].equals("services") && uriParts[5].equals("methods")) {
      //User defined services handle methods on them:
      //Service Path:  "/v2/apps/{app-id}/services/{service-id}/methods/<user-defined-method-path>"
      //Discoverable Service Name -> "service.%s.%s.%s", namespaceId, appId, serviceId
      // also rewrite request to v3 uri now since ServiceHttpServer now announces using v3 URIs
      rewriteRequest(request);
      String serviceName = String.format("service.%s.%s.%s", getAuthenticatedAccountId(request), uriParts[2],
                                         uriParts[4]);
      return serviceName;
    } else {
      return Constants.Service.APP_FABRIC_HTTP;
    }
  }

  private String getV3RoutingService(String [] uriParts, AllowedMethod requestMethod, HttpRequest request) {
    if ((uriParts.length >= 2) && uriParts[1].equals("feeds")) {
      // TODO find a better way to handle that - this looks hackish
      return null;
    } else if ((uriParts.length >= 9) && "services".equals(uriParts[5]) && "methods".equals(uriParts[7])) {
      //User defined services handle methods on them:
      //Path: "/v3/namespaces/{namespace-id}/apps/{app-id}/services/{service-id}/methods/<user-defined-method-path>"
      //Discoverable Service Name -> "service.%s.%s.%s", namespaceId, appId, serviceId
      String serviceName = String.format("service.%s.%s.%s", uriParts[2], uriParts[4], uriParts[6]);
      return serviceName;
    } else if ((uriParts.length >= 4) && uriParts[3].equals("streams")) {
      //     /v3/namespaces/<namespace>/streams goes to AppFabricHttp
      //     /v3/namespaces/<namespace>/streams/<stream-id> PUT, POST should go to Stream Handler
      // GET /v3/namespaces/<namespace>/streams/flows should go to AppFabricHttp
      // All else go to Stream Handler
      if (uriParts.length == 4) {
        return Constants.Service.APP_FABRIC_HTTP;
      } else if (uriParts.length == 5) {
        return Constants.Service.STREAMS;
      } else if ((uriParts.length == 6) && uriParts[3].equals("flows") && requestMethod.equals(AllowedMethod.GET)) {
        return Constants.Service.APP_FABRIC_HTTP;
      } else {
        return Constants.Service.STREAMS;
      }
    } else if (uriParts.length >= 8 && uriParts[7].equals("logs")) {
      //Log Handler Path /v3/namespaces/<namespaceid>apps/<appid>/<programid-type>/<programid>/logs
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

  private HttpRequest rewriteRequest(HttpRequest request) {
    String originalUri = request.getUri();
    String namespacedApiPrefix = String.format("%s/namespaces/%s", Constants.Gateway.API_VERSION_3,
                                               Constants.DEFAULT_NAMESPACE);
    request.setUri(originalUri.replaceFirst(Constants.Gateway.API_VERSION_2, namespacedApiPrefix));
    return request;
  }
}
