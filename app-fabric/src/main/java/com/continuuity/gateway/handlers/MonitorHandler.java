package com.continuuity.gateway.handlers;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Monitor Handler returns the status of different discoverable services
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class MonitorHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MonitorHandler.class);

  private static final String VERSION = Constants.Gateway.GATEWAY_VERSION;

  private final DiscoveryServiceClient discoveryServiceClient;

  /**
   * Number of seconds for timing out a service endpoint discovery.
   */
  private static final long DISCOVERY_TIMEOUT_SECONDS = 3;

  private enum Services {
    METRICS (Constants.Service.METRICS),
    TRANSACTION (Constants.Service.TRANSACTION),
    STREAMS (Constants.Service.STREAM_HANDLER),
    APPFABRIC (Constants.Service.APP_FABRIC_HTTP);

    private final String name;

    private Services(String name) {
      this.name = name;
    }

    public String getName() { return name; }

    public static Services valueofName(String name) { return valueOf(name.toUpperCase()); }
  }

  @Inject
  public MonitorHandler(DiscoveryServiceClient discoveryServiceClient) {
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Path("/monitor/{service-id}")
  @GET
  public void monitor(final HttpRequest request, final HttpResponder responder,
                      @PathParam("service-id") final String service) {
    if (discoverService(service)) {
      //Service is discoverable
      responder.sendString(HttpResponseStatus.OK, service + " is OK\n");
    } else {
      responder.sendString(HttpResponseStatus.NOT_FOUND, service + " not found\n");
    }
  }

  private boolean discoverService(String serviceName) {
    try {
      Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(
        Services.valueofName(serviceName).getName());
      Discoverable discoverable = new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoverables),
                                                                DISCOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS).pick();
      return (discoverable != null);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
