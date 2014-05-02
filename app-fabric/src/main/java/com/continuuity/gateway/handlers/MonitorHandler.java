package com.continuuity.gateway.handlers;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.ning.http.client.Response;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Future;
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
   * Timeout to get response from discovered service.
   */
  private static final long SERVICE_PING_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

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
      if (discoverable == null) {
        return false;
      } else {
        // Ping the discovered service to check its status
        String url = String.format("http://%s:%d/ping", discoverable.getSocketAddress().getHostName(),
                                   discoverable.getSocketAddress().getPort());
        //TODO: Figure out a way to Ping Transaction Service (Hint: Used RUOK?)
        if (!Services.valueofName(serviceName).equals(Services.TRANSACTION)) {
          checkGetStatus(url);
        }
        return true;
      }
    } catch (IllegalArgumentException e) {
      return false;
    } catch (Exception e) {
      LOG.warn("Unable to ping {} : Reason : {}", serviceName, e.getMessage());
      return false;
    }
  }

  private void checkGetStatus(String url) throws Exception {
    SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
      .setUrl(url)
      .setRequestTimeoutInMs((int) SERVICE_PING_RESPONSE_TIMEOUT)
      .build();

    try {
      Future<Response> future = client.get();
      Response response = future.get(SERVICE_PING_RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
      if (!HttpResponseStatus.valueOf(response.getStatusCode()).equals(HttpResponseStatus.OK)) {
        throw new Exception(response.getResponseBody());
      }
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      client.close();
    }
  }
}
