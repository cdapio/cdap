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

  public String getRoutingPath(String requestPath, HttpRequest httpRequest) {
    try {
      String method = httpRequest.getMethod().getName();
      AllowedMethod requestMethod = AllowedMethod.valueOf(method);
      String[] uriParts = StringUtils.split(requestPath, '/');
      if ((uriParts.length >= 2) && uriParts[1].equals("metrics")) {
        return Constants.Service.METRICS;
      } else if ((uriParts.length >= 2) && uriParts[1].equals("streams")) {
        // /v2/streams/<stream-id> GET should go to AppFabricHttp, PUT, POST should go to Stream Handler
        // /v2/streams should go to AppFabricHttp
        // GET /v2/streams/flows should go to AppFabricHttp, rest should go Stream Handler
        if (uriParts.length == 2) {
          return Constants.Service.APP_FABRIC_HTTP;
        } else if (uriParts.length == 3) {
          return (requestMethod.equals(AllowedMethod.GET)) ?
            Constants.Service.APP_FABRIC_HTTP : Constants.Service.STREAM_HANDLER;
        } else if ((uriParts.length == 4) && uriParts[3].equals("flows") && requestMethod.equals(AllowedMethod.GET)) {
          return Constants.Service.APP_FABRIC_HTTP;
        } else {
          return Constants.Service.STREAM_HANDLER;
        }
      } else if ((uriParts.length >= 6) && uriParts[5].equals("logs")) {
        //Log Handler Path /v2/apps/<appid>/<programid-type>/<programid>/logs
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
