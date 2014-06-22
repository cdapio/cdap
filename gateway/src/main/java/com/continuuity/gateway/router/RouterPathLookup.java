package com.continuuity.gateway.router;

import com.continuuity.common.conf.Constants;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.AuthenticatedHttpHandler;
import com.google.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.jboss.netty.handler.codec.http.HttpRequest;

/**
 * Class to match the request path to corresponding service like app-fabric, or metrics service.
 */
public final class RouterPathLookup extends AuthenticatedHttpHandler {

  @Inject
  public RouterPathLookup(Authenticator authenticator) { super(authenticator); }

  private enum AllowedMethod {
    GET, PUT, POST, DELETE
  }

  /**
   * Returns the reactor service which will handle the HttpRequest
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
      //But procedure/stream calls issued by the UI should be routed to the appropriate reactor service
      if (fallbackService.contains("$HOST") && (uriParts.length >= 1)
                                            && (!(("/" + uriParts[0]).equals(Constants.Gateway.GATEWAY_VERSION)))) {
        return fallbackService;
      }

      if ((uriParts.length >= 2) && uriParts[1].equals("metrics")) {
        return Constants.Service.METRICS;
      } else if ((uriParts.length >= 2) && uriParts[1].equals("data")) {
        if ((uriParts.length >= 3) && uriParts[2].equals("queries")) {
          return Constants.Service.EXPLORE_HTTP_USER_SERVICE;
        }
        return Constants.Service.DATASET_MANAGER;
      } else if ((uriParts.length == 3) && uriParts[1].equals("explore") && uriParts[2].equals("status")) {
        return Constants.Service.EXPLORE_HTTP_USER_SERVICE;
      } else if ((uriParts.length >= 2) && uriParts[1].equals("streams")) {
        // /v2/streams/<stream-id> GET should go to AppFabricHttp, PUT, POST should go to Stream Handler
        // /v2/streams should go to AppFabricHttp
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
        String accId = getAuthenticatedAccountId(httpRequest);
        //Discoverable Service Name -> procedure.%s.%s.%s", accountId, appId, procedureName ;
        String serviceName = String.format("procedure.%s.%s.%s", accId, uriParts[2], uriParts[4]);
        return serviceName;
      } else {
        return Constants.Service.APP_FABRIC_HTTP;
      }
    } catch (Exception e) {

    }
    return Constants.Service.APP_FABRIC_HTTP;
  }
}
