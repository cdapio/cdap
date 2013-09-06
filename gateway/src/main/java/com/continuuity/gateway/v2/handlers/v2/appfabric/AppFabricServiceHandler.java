package com.continuuity.gateway.v2.handlers.v2.appfabric;

import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.ArchiveId;
import com.continuuity.app.services.ArchiveInfo;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.FlowDescriptor;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.FlowRunRecord;
import com.continuuity.app.services.FlowStatus;
import com.continuuity.app.services.ProgramDescriptor;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.ProgramRunRecord;
import com.continuuity.app.services.ProgramStatus;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.conf.Services;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.service.ServerException;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.v2.handlers.v2.AuthenticatedHttpHandler;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;

import static com.continuuity.common.metrics.MetricsHelper.Status.BadRequest;

/**
 *  {@link AppFabricServiceHandler} is REST interface to AppFabric backend.
 */
@Path("/v2")
public class AppFabricServiceHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServiceHandler.class);
  private final DiscoveryServiceClient discoveryClient;
  private static final String ARCHIVE_NAME_HEADER = "X-Archive-Name";
  private final EndpointStrategy endpointStrategy;

  @Inject
  public AppFabricServiceHandler(GatewayAuthenticator authenticator, DiscoveryServiceClient discoveryClient) {
    super(authenticator);
    this.discoveryClient = discoveryClient;
    this.endpointStrategy = new RandomEndpointStrategy(discoveryClient.discover(Services.APP_FABRIC));
  }

  @PUT
  @Path("/apps")
  public void deploy(HttpRequest request, HttpResponder responder) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      String archiveName = request.getHeader(ARCHIVE_NAME_HEADER);

      if (archiveName == null || archiveName.isEmpty()) {
        responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
        return;
      }

      ChannelBuffer content = request.getContent();
      if (content == null) {
        responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
        return;
      }

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Services.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      try {
        ArchiveInfo rInfo = new ArchiveInfo(accountId, "gateway", archiveName);
        ArchiveId rIdentifier = client.init(token, rInfo);

        while (content.readableBytes() > 0) {
          int bytesToRead = Math.min(1024 * 1024, content.readableBytes());
          client.chunk(token, rIdentifier, content.readSlice(bytesToRead).toByteBuffer());
        }

        client.deploy(token, rIdentifier);
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  /**
   * Deletes an application specified by appId
   */
  @DELETE
  @Path("/apps/{appId}")
  public void deleteApp(HttpRequest request, HttpResponder responder,
                        @PathParam("appId") final String appId) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Services.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        client.removeApplication(token, new ProgramId(accountId, appId, ""));
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  /**
   * Deletes all applications in the reactor.
   */
  @DELETE
  @Path("/apps")
  public void deleteAllApps(HttpRequest request, HttpResponder responder) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Services.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        client.removeAll(token, accountId);
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @GET
  @Path("/apps/{app-id}/runnables/{id}/history")
  public void runnableHistory(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId, @PathParam("id") final String id) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      JsonArray result = getRunnableHistory(accountId, appId, id);
      responder.sendJson(HttpResponseStatus.OK, result);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/instances")
  public void getFlowletInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") final String appId, @PathParam("flow-id") final String flowId,
                                  @PathParam("flowlet-id") final String flowletId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Services.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        int count = client.getInstances(token, new ProgramId(accountId, appId, flowId), flowletId);
        JsonObject o = new JsonObject();
        o.addProperty("instances", count);
        responder.sendJson(HttpResponseStatus.OK, o);
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @PUT
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/instances/{instance-count}")
  public void setFlowletInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") final String appId, @PathParam("flow-id") final String flowId,
                                  @PathParam("flowlet-id") final String flowletId,
                                  @PathParam("instance-count") final String instanceCount) {
    short instances = 0;
    try {
      Short count = Short.parseShort(instanceCount);
      instances = count.shortValue();
      if (instances < 1) {
        responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
        return;
      }
    } catch (NumberFormatException e) {
      responder.sendStatus(HttpResponseStatus.BAD_REQUEST);
      return;
    }

    try {
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Services.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        client.setInstances(token, new ProgramId(accountId, appId, flowId), flowletId, instances);
        responder.sendStatus(HttpResponseStatus.OK);
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }


  @GET
  @Path("/apps/{app-id}/runnables/{id}/status")
  public void runnableStatus(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") final String appId, @PathParam("id") final String id) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Services.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        ProgramStatus status = client.status(token, new ProgramId(accountId, appId, ""));
        JsonObject o = new JsonObject();
        o.addProperty("status", status.getStatus());
        responder.sendJson(HttpResponseStatus.OK, o);
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @GET
  @Path("/apps/{app-id}/runnables/{id}")
  public void runnableSpecification(HttpRequest request, HttpResponder responder,
                                    @PathParam("app-id") final String appId, @PathParam("id") final String id) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      TProtocol protocol =  getThriftProtocol(Services.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        String specification = client.getSpecification(new ProgramId(accountId, appId, ""));
        responder.sendByteArray(HttpResponseStatus.OK, specification.getBytes(Charsets.UTF_8),
                                ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }

  @POST
  @Path("/apps/{app-id}/runnables/{id}/start")
  public void runnableStart(HttpRequest request, HttpResponder responder,
                            @PathParam("app-id") final String appId, @PathParam("id") final String id) {
    runnableStartStop(request, responder, appId, id, "start");
  }

  @POST
  @Path("/apps/{app-id}/runnables/{id}/stop")
  public void runnableStop(HttpRequest request, HttpResponder responder,
                            @PathParam("app-id") final String appId, @PathParam("id") final String id) {
    runnableStartStop(request, responder, appId, id, "stop");
  }


  private void runnableStartStop(HttpRequest request, HttpResponder responder,
                                 String appId, String id, String action) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Services.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        if ("start".equals(action)) {
          client.start(token, new ProgramDescriptor(new ProgramId(accountId, appId, ""), null));
        } else if ("stop".equals(action)) {
          client.stop(token, new ProgramId(accountId, appId, ""));
        }
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    }
  }


  private JsonArray getRunnableHistory(String accountId, String appId, String runnableid) throws Exception {
    TProtocol protocol =  getThriftProtocol(Services.APP_FABRIC, endpointStrategy);
    AppFabricService.Client client = new AppFabricService.Client(protocol);
    try {
      List<ProgramRunRecord> records = client.getHistory(new ProgramId(accountId, appId, runnableid));
      JsonArray history = new JsonArray();
      for (ProgramRunRecord record : records) {
        JsonObject object = new JsonObject();
        object.addProperty("runid", record.getRunId());
        object.addProperty("start", record.getStartTime());
        object.addProperty("end", record.getEndTime());
        object.addProperty("status", record.getEndStatus());
        history.add(object);
      }
      return history;
    } finally {
      if (client.getInputProtocol().getTransport().isOpen()) {
        client.getInputProtocol().getTransport().close();
      }
      if (client.getOutputProtocol().getTransport().isOpen()) {
        client.getOutputProtocol().getTransport().close();
      }
    }
  }
}
