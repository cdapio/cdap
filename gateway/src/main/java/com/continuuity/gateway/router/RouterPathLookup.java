package com.continuuity.gateway.router;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.ImmutablePair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jboss.netty.handler.codec.http.HttpMethod;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class to match the request path to corresponding service like app-fabric, or metrics service.
 */

public final class RouterPathLookup {
  private static final String VERSION = Constants.Gateway.GATEWAY_VERSION;

  private static final String COMMON_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/(flows|procedures|mapreduce|workflows)/([A-Za-z0-9_]+)/" +
    "(start|debug|stop|status|history|runtimeargs)";
  private static final String DEPLOY_PATH = VERSION +
    "/?/apps/?([A-Za-z0-9_]+)?/?$";
  private static final String DEPLOY_STATUS_PATH = VERSION +
    "/?/deploy/status/?";
  private static final String METRICS_PATH = "^" + VERSION +
    "/metrics";
  private static final String LOGHANDLER_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/(flows|procedures|mapreduce|workflows)/([A-Za-z0-9_]+)/logs";

  private static final String FLOWLET_INSTANCE_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/flows/([A-Za-z0-9_]+)/flowlets/([A-Za-z0-9_]+)/instances";

  private static final String SCHEDULER_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/workflows/([A-Za-z0-9_]+)/" +
    "(schedules|nextruntime)";

  private static final String LIVEINFO_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/(flows|procedures)/([A-Za-z0-9_]+)/live-info";

  //TODO: Consolidate this!!!
  private static final String SPEC_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/(flows|procedures|mapreduce|workflows)/([A-Za-z0-9_]+)";

  private static final Map<String, HttpMethod> ALLOWED_METHODS_MAP = ImmutableMap.of("GET", HttpMethod.GET,
                                                                                     "PUT", HttpMethod.PUT,
                                                                                     "POST", HttpMethod.POST);

  private static final ImmutableMap<ImmutablePair<List<HttpMethod>, Pattern>, String> ROUTING_MAP =
    ImmutableMap.<ImmutablePair<List<HttpMethod>, Pattern>, String>builder()
      .put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET),
                                                        Pattern.compile(COMMON_PATH)),
                                                        Constants.Service.APP_FABRIC_HTTP)
      .put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET),
                                                        Pattern.compile(SCHEDULER_PATH)),
                                                        Constants.Service.APP_FABRIC_HTTP)
      .put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.POST, HttpMethod.PUT),
                                                        Pattern.compile(DEPLOY_PATH)),
                                                        Constants.Service.APP_FABRIC_HTTP)
      .put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET),
                                                        Pattern.compile(DEPLOY_STATUS_PATH)),
                                                        Constants.Service.APP_FABRIC_HTTP)
      .put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET, HttpMethod.PUT),
                                                        Pattern.compile(FLOWLET_INSTANCE_PATH)),
                                                        Constants.Service.APP_FABRIC_HTTP)
      .put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET, HttpMethod.PUT),
                                                        Pattern.compile(SPEC_PATH)),
                                                        Constants.Service.APP_FABRIC_HTTP)
      .put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET, HttpMethod.PUT),
                                                        Pattern.compile(LIVEINFO_PATH)),
                                                        Constants.Service.APP_FABRIC_HTTP)
      .put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(
                                                        HttpMethod.GET, HttpMethod.PUT, HttpMethod.POST),
                                                        Pattern.compile(METRICS_PATH)),
                                                        Constants.Service.METRICS)
      .put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET),
                                                        Pattern.compile(LOGHANDLER_PATH)),
                                                        Constants.Service.METRICS)
      .build();

  public static String getRoutingPath(String requestPath, String method) {
    if (!ALLOWED_METHODS_MAP.containsKey(method)) {
      return null;
    }

    for (Map.Entry<ImmutablePair<List<HttpMethod>, Pattern>, String> uriPattern : ROUTING_MAP.entrySet()) {
      Matcher match = uriPattern.getKey().getSecond().matcher(requestPath);
      if (match.find()) {
        if (uriPattern.getKey().getFirst().contains(ALLOWED_METHODS_MAP.get(method))) {
          return uriPattern.getValue();
        }
      }
    }
    return null;
  }
}
