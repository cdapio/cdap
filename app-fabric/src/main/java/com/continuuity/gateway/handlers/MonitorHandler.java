package com.continuuity.gateway.handlers;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.gateway.handlers.util.ThriftHelper;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.ning.http.client.Response;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.thrift.protocol.TProtocol;
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

import static com.continuuity.data2.transaction.distributed.thrift.TTransactionServer.Client;

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
      String response = String.format("%s is OK\n", service);
      responder.sendString(HttpResponseStatus.OK, response);
    } else {
      String response = String.format("%s not found\n", service);
      responder.sendString(HttpResponseStatus.NOT_FOUND, service);
    }
  }

  private boolean discoverService(String serviceName) {
    try {
      Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(Services.valueofName(
        serviceName).getName());
      EndpointStrategy endpointStrategy = new TimeLimitEndpointStrategy(
        new RandomEndpointStrategy(discoverables), DISCOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      Discoverable discoverable = endpointStrategy.pick();
      //Transaction Service will return null discoverable in SingleNode mode
      if (discoverable == null) {
        return false;
      }

      //Ping the discovered service to check its status.
      //Use Thrift Client for Transaction and HTTP PingHandlers for other services
      if (!Services.valueofName(serviceName).equals(Services.TRANSACTION)) {
        String url = String.format("http://%s:%d/ping", discoverable.getSocketAddress().getHostName(),
                                   discoverable.getSocketAddress().getPort());
        return checkGetStatus(url).equals(HttpResponseStatus.OK);
      } else {
        TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.TRANSACTION, endpointStrategy);
        Client client = new Client(protocol);
        return client.status().equals("OK");
      }
    } catch (IllegalArgumentException e) {
      return false;
    } catch (Exception e) {
      LOG.warn("Unable to ping {} : Reason : {}", serviceName, e.getMessage());
      return false;
    }
  }

  private HttpResponseStatus checkGetStatus(String url) throws Exception {
    SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
      .setUrl(url)
      .setRequestTimeoutInMs((int) SERVICE_PING_RESPONSE_TIMEOUT)
      .build();

    try {
      Future<Response> future = client.get();
      Response response = future.get(SERVICE_PING_RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
      return HttpResponseStatus.valueOf(response.getStatusCode());
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      client.close();
    }
    return HttpResponseStatus.NOT_FOUND;
  }
}
