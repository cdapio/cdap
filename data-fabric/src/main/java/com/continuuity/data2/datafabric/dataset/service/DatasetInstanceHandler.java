package com.continuuity.data2.datafabric.dataset.service;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.data2.datafabric.dataset.instance.DatasetInstanceManager;
import com.continuuity.data2.datafabric.dataset.type.DatasetTypeManager;
import com.continuuity.data2.dataset2.user.AdminOpResponse;
import com.continuuity.data2.transaction.distributed.RetryWithBackoff;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.data.dataset.DatasetDefinition;
import com.continuuity.internal.data.dataset.DatasetInstanceProperties;
import com.continuuity.internal.data.dataset.DatasetInstanceSpec;

import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handles dataset instance management calls.
 */
// todo: do we want to make it authenticated? or do we treat it always as "internal" piece?
@Path(Constants.Gateway.GATEWAY_VERSION)
public class DatasetInstanceHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(DatasetInstanceHandler.class);
  private static final Gson GSON = new Gson();

  private final DatasetTypeManager implManager;
  private final DatasetInstanceManager instanceManager;
  private final DiscoveryServiceClient discoveryClient;
  private InetSocketAddress datasetUserAddress;

  @Inject
  public DatasetInstanceHandler(DiscoveryServiceClient discoveryClient,
                                DatasetTypeManager implManager, DatasetInstanceManager instanceManager) {
    this.discoveryClient = discoveryClient;
    this.implManager = implManager;
    this.instanceManager = instanceManager;
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting DatasetInstanceHandler");

    // TODO(alvin): remove this once user service is run on-demand
    EndpointStrategy endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.DATASET_USER)),
      5L, TimeUnit.SECONDS);
    datasetUserAddress = endpointStrategy.pick().getSocketAddress();
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping DatasetInstanceHandler");
  }

  @GET
  @Path("/data/instances/")
  public void list(HttpRequest request, final HttpResponder responder) {
    responder.sendJson(HttpResponseStatus.OK, instanceManager.getAll());
  }

  @DELETE
  @Path("/data/instances/")
  public void deleteAll(HttpRequest request, final HttpResponder responder) {
    instanceManager.deleteAll();
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/data/instances/{instance-name}")
  public void getInfo(HttpRequest request, final HttpResponder responder,
                      @PathParam("instance-name") String name) {
    DatasetInstanceSpec spec = instanceManager.get(name);
    if (spec == null) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } else {
      DatasetInstanceMeta info = new DatasetInstanceMeta(spec, implManager.getTypeInfo(spec.getType()));
      responder.sendJson(HttpResponseStatus.OK, info);
    }
  }

  @POST
  @Path("/data/instances/{instance-name}")
  public void add(HttpRequest request, final HttpResponder responder,
                  @PathParam("instance-name") String name) {
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()));
    DatasetInstanceProperties props = GSON.fromJson(reader, DatasetInstanceProperties.class);
    String typeName = request.getHeader("type-name");

    LOG.info("Creating dataset instance {}, type name: {}, props: {}", name, typeName, props);

    DatasetInstanceSpec existing = instanceManager.get(name);
    if (existing != null) {
      String message = String.format("Cannot create dataset instance %s: instance with same name already exists %s",
                                     name, existing);
      LOG.warn(message);
      responder.sendString(HttpResponseStatus.CONFLICT, message);
      return;
    }

    DatasetDefinition type = implManager.getType(typeName);

    if (type == null) {
      String message = String.format("Cannot create dataset instance %s: unknown type %s",
                                     name, typeName);
      LOG.warn(message);
      responder.sendString(HttpResponseStatus.NOT_FOUND, message);
      return;
    }

    instanceManager.add(type.configure(name, props));
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @DELETE
  @Path("/data/instances/{instance-name}")
  public void drop(HttpRequest request, final HttpResponder responder,
                       @PathParam("instance-name") String instanceName) {
    LOG.info("Deleting dataset instance {}", instanceName);

    if (!instanceManager.delete(instanceName)) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      return;
    }
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @POST
  @Path("/data/instances/{instance-id}/admin/{method}")
  public void executeAdmin(HttpRequest request, final HttpResponder responder,
                           @PathParam("instance-id") String instanceName,
                           @PathParam("method") String method) {
    String template = "http://%s:%d%s/data/instances/%s/admin/%s";
    String urlString = String.format(template, datasetUserAddress.getAddress().getCanonicalHostName(),
                                     datasetUserAddress.getPort(), Constants.Gateway.GATEWAY_VERSION,
                                     instanceName, method);
    HttpURLConnection connection = null;

    try {
      connection = (HttpURLConnection) new URL(urlString).openConnection();
      connection.setRequestMethod("POST");

      int responseCode = connection.getResponseCode();
      AdminOpResponse response = GSON.fromJson(
        new InputStreamReader(connection.getInputStream()), AdminOpResponse.class);

      responder.sendJson(HttpResponseStatus.valueOf(responseCode), response);
    } catch (IOException e) {
      LOG.error("Error opening connection to {}", urlString);
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } finally {
      if (connection != null) {
        connection.disconnect();
      }
    }
  }

  @POST
  @Path("/data/instances/{instance-id}/data/{method}")
  public void executeDataOp(HttpRequest request, final HttpResponder responder,
                           @PathParam("instance-id") String instanceName,
                           @PathParam("method") String method) {
    // todo: execute data operation
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }

}
