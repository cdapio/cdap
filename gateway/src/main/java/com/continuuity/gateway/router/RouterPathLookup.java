package com.continuuity.gateway.router;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.AuthenticatedHttpHandler;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to match the request path to corresponding service like app-fabric, or metrics service.
 */

public final class RouterPathLookup extends AuthenticatedHttpHandler {

  @Inject
  public RouterPathLookup(Authenticator authenticator) {
    super(authenticator);
  }

  private static final String VERSION = Constants.Gateway.GATEWAY_VERSION;

  private static final String COMMON_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_-]+)/(flows|procedures|mapreduce|workflows)/([A-Za-z0-9_-]+)/" +
    "(start|debug|stop|status|history|runtimeargs)";

  private static final String PROGRAMINFO_PATH = VERSION +
    "/?/(flows|procedures|mapreduce|workflows)/?$";

  private static final String ALLAPPINFO_PATH = VERSION +
    "/?/apps/?$";

  private static final String APPINFO_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_-]+)/?(flows|procedures|mapreduce|workflows)?/?$";
  private static final String DELETE_PATH = VERSION +
    "/?/apps/?";
  private static final String DEPLOY_PATH = VERSION +
    "/?/apps/?([A-Za-z0-9_]+)?/?$";

  private static final String WEBAPP_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/webapp/(status|start|stop)";

  private static final String DEPLOY_STATUS_PATH = VERSION +
    "/?/deploy/status/?";
  private static final String METRICS_PATH = "^" + VERSION +
    "/metrics";
  private static final String LOGHANDLER_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_-]+)/(flows|procedures|mapreduce|workflows)/([A-Za-z0-9_-]+)/logs";

  private static final String PROCEDURE_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_-]+)/procedures/([A-Za-z0-9_-]+)/methods/(.+)";

  private static final String FLOWLET_INSTANCE_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_-]+)/flows/([A-Za-z0-9_-]+)/flowlets/([A-Za-z0-9_-]+)/instances";

  private static final String PROCEDURE_INSTANCE_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_-]+)/procedures/(A-Za-z0-9_-]+)/instances/?$";

  private static final String TRANSACTIONS_STATE_PATH = VERSION +
    "/transactions/state";

  private static final String TRANSACTION_ID_PATH = VERSION +
    "/transactions/([A-Za-z0-9_]+)/invalidate";

  private static final String SCHEDULER_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_-]+)/workflows/([A-Za-z0-9_-]+)/" +
    "(schedules|nextruntime)";

  private static final String LIVEINFO_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_-]+)/(flows|procedures)/([A-Za-z0-9_-]+)/live-info";

  private static final String ALLDATA_PATH = VERSION +
    "/(streams|datasets)/?$";

  private static final String DATA_PATH = VERSION +
    "/(streams|datasets)/([A-Za-z0-9_-]+)/?$";

  private static final String APPDATA_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_-]+)/(streams|datasets)/?$";

  private static final String FLOWINFO_PATH = VERSION +
    "/(streams|datasets)/([A-Za-z0-9_-]+)/flows/?$";

  //TODO: Consolidate this!!!
  private static final String SPEC_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_-]+)/(flows|procedures|mapreduce|workflows)/([A-Za-z0-9_-]+)/?$";

  private static final String PROMOTE_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/promote";
  private static final String RESET_PATH = VERSION +
    "/unrecoverable/reset";

  private enum AllowedMethod {
    GET, PUT, POST, DELETE
  }

  private static final ImmutableMap<ImmutablePair<? extends Set<AllowedMethod>, Pattern>, String> ROUTING_MAP =
    ImmutableMap.<ImmutablePair<? extends Set<AllowedMethod>, Pattern>, String>builder()
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET), Pattern.compile(COMMON_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET), Pattern.compile(SCHEDULER_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.range(AllowedMethod.PUT, AllowedMethod.POST), Pattern.compile(DEPLOY_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET), Pattern.compile(DEPLOY_STATUS_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET, AllowedMethod.PUT), Pattern.compile(FLOWLET_INSTANCE_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET, AllowedMethod.PUT), Pattern.compile(PROCEDURE_INSTANCE_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET, AllowedMethod.PUT), Pattern.compile(SPEC_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET, AllowedMethod.PUT), Pattern.compile(LIVEINFO_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET, AllowedMethod.POST, AllowedMethod.DELETE),
           Pattern.compile(METRICS_PATH)),
           Constants.Service.METRICS)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET), Pattern.compile(LOGHANDLER_PATH)),
           Constants.Service.METRICS)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.DELETE), Pattern.compile(DELETE_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET), Pattern.compile(PROGRAMINFO_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET), Pattern.compile(ALLAPPINFO_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET), Pattern.compile(APPINFO_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      // todo change to Constants.Service.DATASET_MANAGER
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET, AllowedMethod.POST),
                            Pattern.compile(TRANSACTIONS_STATE_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      // todo change to Constants.Service.DATASET_MANAGER
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.POST), Pattern.compile(TRANSACTION_ID_PATH)),
          Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET), Pattern.compile(ALLDATA_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET), Pattern.compile(DATA_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET), Pattern.compile(APPDATA_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET), Pattern.compile(FLOWINFO_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.POST), Pattern.compile(RESET_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.POST), Pattern.compile(PROMOTE_PATH)),
           Constants.Service.APP_FABRIC_HTTP)
      .put(ImmutablePair.of(EnumSet.of(AllowedMethod.GET, AllowedMethod.POST), Pattern.compile(PROCEDURE_PATH)),
           Constants.Service.PROCEDURES)
      .build();

  public String getRoutingPath(String requestPath, HttpRequest httpRequest) {
    try {
      String method = httpRequest.getMethod().getName();
      AllowedMethod requestMethod = AllowedMethod.valueOf(method);

      Set<Map.Entry<ImmutablePair<? extends Set<AllowedMethod>, Pattern>, String>> entries = ROUTING_MAP.entrySet();
      for (Map.Entry<ImmutablePair<? extends Set<AllowedMethod>, Pattern>, String> uriPattern : entries) {
        Matcher match = uriPattern.getKey().getSecond().matcher(requestPath);
        if (match.find()) {
          if (uriPattern.getKey().getFirst().contains(requestMethod)) {
            if (uriPattern.getValue() == Constants.Service.PROCEDURES) {
              String accId = getAuthenticatedAccountId(httpRequest);
              //Discoverable Service Name -> procedure.%s.%s.%s", accountId, appId, procedureName ;
              String serviceName = String.format("procedure.%s.%s.%s", accId, match.group(1), match.group(2));
              return serviceName;
            }
            return uriPattern.getValue();
          }
        }
      }
    } catch (IllegalArgumentException e) {
      // Method not supported
    }

    return null;
  }
}
