package com.continuuity.gateway.router;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.utils.ImmutablePair;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.jboss.netty.handler.codec.http.HttpMethod;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



/**
 * Class to match the request path to corresponding service like app-fabric, or metrics service.
 */
public final class RouterPathLookup {
  private static final String VERSION = Constants.Gateway.GATEWAY_VERSION;

  private static final String STATUS_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/(flows|procedures|mapreduce|workflows)/([A-Za-z0-9_]+)/status";
  private static final String STARTSTOP_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/(flows|procedures|mapreduce|workflows)/([A-Za-z0-9_]+)/(start|stop)";
  private static final String HISTORY_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/(flows|procedures|mapreduce|workflows)/([A-Za-z0-9_]+)/history";
  private static final String RUNTIMEARGS_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/(flows|procedures|mapreduce|workflows)/([A-Za-z0-9_]+)/runtimeargs";
  private static final String DEPLOY_PATH = VERSION +
    "/?/apps/?([A-Za-z0-9_]+)?/?$";
  private static final String DEPLOY_STATUS_PATH = VERSION +
    "/?/deploy/status/?";
  private static final String METRICS_PATH = "^" + VERSION +
    "/metrics";
  private static final String LOGHANDLER_PATH = VERSION +
    "/?/apps/([A-Za-z0-9_]+)/(flows|procedures|mapreduce|workflows)/([A-Za-z0-9_]+)/logs";

  private static final Map<String, HttpMethod> ALLOWED_METHODS_MAP = ImmutableMap.of("GET", HttpMethod.GET,
                                                                                     "PUT", HttpMethod.PUT,
                                                                                     "POST", HttpMethod.POST);

  private static Map<ImmutablePair<List<HttpMethod>, Pattern>, String> ROUTING_MAP = null;

  public static void init() {
    if (ROUTING_MAP == null) {
      ROUTING_MAP = new HashMap<ImmutablePair<List<HttpMethod>, Pattern>, String>();
      ROUTING_MAP.put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET),
              Pattern.compile(STATUS_PATH)),
          Constants.Service.APP_FABRIC_HTTP);
      ROUTING_MAP.put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.POST),
              Pattern.compile(STARTSTOP_PATH)),
          Constants.Service.APP_FABRIC_HTTP);
      ROUTING_MAP.put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET),
              Pattern.compile(HISTORY_PATH)),
          Constants.Service.APP_FABRIC_HTTP);
      ROUTING_MAP.put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET, HttpMethod.PUT),
              Pattern.compile(RUNTIMEARGS_PATH)),
          Constants.Service.APP_FABRIC_HTTP);
      ROUTING_MAP.put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET, HttpMethod.PUT),
              Pattern.compile(DEPLOY_PATH)),
          Constants.Service.APP_FABRIC_HTTP);
      ROUTING_MAP.put(new ImmutablePair<List<HttpMethod>, Pattern>(ImmutableList.of(HttpMethod.GET),
              Pattern.compile(DEPLOY_STATUS_PATH)),
          Constants.Service.APP_FABRIC_HTTP);
      ROUTING_MAP.put(new ImmutablePair<List<HttpMethod>, Pattern>(
              ImmutableList.of(HttpMethod.GET, HttpMethod.POST, HttpMethod.PUT),
              Pattern.compile(METRICS_PATH)),
          Constants.Service.METRICS);
      ROUTING_MAP.put(new ImmutablePair<List<HttpMethod>, Pattern>(
              ImmutableList.of(HttpMethod.GET),
              Pattern.compile(LOGHANDLER_PATH)),
          Constants.Service.METRICS);
    }
  }

  public static String getRoutingPath(String requestPath, String method){

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
