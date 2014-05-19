package com.continuuity.gateway.handlers;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.data2.transaction.TransactionSystemClient;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.ning.http.client.Response;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private final TransactionSystemClient txClient;
  private static final java.lang.reflect.Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  /**
   * Timeout to get response from discovered service.
   */
  private static final long SERVICE_PING_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

  /**
   * Number of seconds for timing out a service endpoint discovery.
   */
  private static final long DISCOVERY_TIMEOUT_SECONDS = 1;

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
  public MonitorHandler(DiscoveryServiceClient discoveryServiceClient, TransactionSystemClient txClient) {
    this.discoveryServiceClient = discoveryServiceClient;
    this.txClient = txClient;
  }

  @Path("/system/services/status")
  @GET
  public void getBootStatus(final HttpRequest request, final HttpResponder responder) {
    List<Map<String, String>> result = Lists.newArrayList();
    String json;
    for (Services service : Services.values()) {
      String serviceName = String.valueOf(service);
      LOG.warn("Checking status of {}", serviceName);
      String status = discoverService(serviceName) ? "OK" : "NOTOK";
      Map<String, String> statusMap = new HashMap<String, String>();
      statusMap.put(serviceName, status);
      result.add(statusMap);
    }

    json = (new Gson()).toJson(result);
    responder.sendByteArray(HttpResponseStatus.OK, json.getBytes(Charsets.UTF_8),
                            ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
  }

  @Path("/system/services/{service-id}/status")
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

      //For Transaction Service use the TransactionSystemClient to check the txManager's status
      if (Services.valueofName(serviceName).equals(Services.TRANSACTION)) {
        return txClient.status().equals("OK");
      }

      //Ping the discovered service to check its status.
      String url = String.format("http://%s:%d/ping", discoverable.getSocketAddress().getHostName(),
                                 discoverable.getSocketAddress().getPort());
      return checkGetStatus(url).equals(HttpResponseStatus.OK);
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
