package com.continuuity.gateway.router;
import com.continuuity.common.conf.Constants;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Map;


/**
 * Class to match the request path to corresponding service like app-fabric, or metrics service.
 */
public final class RouterPathLookup {
  private static final Logger LOG =  LoggerFactory.getLogger(RouterServiceLookup.class);
  private static final String VERSION = Constants.Gateway.GATEWAY_VERSION;

  private static final String STATUS_PATH = VERSION +
      "/?/apps/([A-Za-z0-9_]+)/(flows|procedures|mapreduce|workflows)/([A-Za-z0-9_]+)/(status|start|stop)";
  private static final String STARTSTOP_PATH = VERSION +
      "/?/apps/([A-Za-z0-9_]+)/flows/([A-Za-z0-9_]+)/(start|stop)";

 private static final Map<Pattern, String> RoutingPath = ImmutableMap.of(
                                                         Pattern.compile(STATUS_PATH),
                                                         Constants.Service.APP_FABRIC_HTTP,
                                                         Pattern.compile(STARTSTOP_PATH),
                                                         Constants.Service.APP_FABRIC_HTTP
                                                         );

  public static String getRoutingPath(String requestPath){

    for (Map.Entry<Pattern, String> uriPattern : RoutingPath.entrySet()) {
      Matcher match = uriPattern.getKey().matcher(requestPath);
      if (match.find()) {
        LOG.info("Status call from RouterPathLookup. matched {}", requestPath);
        return uriPattern.getValue();
      }
    }
    return null;
   }

}
