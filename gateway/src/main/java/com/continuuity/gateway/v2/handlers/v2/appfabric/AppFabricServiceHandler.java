package com.continuuity.gateway.v2.handlers.v2.appfabric;

import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.FlowIdentifier;
import com.continuuity.app.services.FlowRunRecord;
import com.continuuity.app.services.ResourceIdentifier;
import com.continuuity.app.services.ResourceInfo;
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
        ResourceInfo rInfo = new ResourceInfo(accountId, "gateway", archiveName, 1, System.currentTimeMillis() / 1000);
        ResourceIdentifier rIdentifier = client.init(token, rInfo);

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
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
        client.removeApplication(token, new FlowIdentifier(accountId, appId, "", 1));
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
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Deletes all applications in the reactor.
   */
  @DELETE
  @Path("/apps")
  public void deleteApp(HttpRequest request, HttpResponder responder) {

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
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/history")
  public void getFlowHistory(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") final String appId, @PathParam("flow-id") final String flowId) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      JsonArray result = getRunnableHistory(accountId, appId, flowId);
      responder.sendJson(HttpResponseStatus.OK, result);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}/history")
  public void getProcedureHistory(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") final String appId,
                             @PathParam("procedure-id") final String procedureId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      JsonArray result = getRunnableHistory(accountId, appId, procedureId);
      responder.sendJson(HttpResponseStatus.OK, result);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/apps/{app-id}/mapreduces/{mapreduce-id}/history")
  public void getMapReduceHistory(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") final String appId,
                                  @PathParam("mapreduce-id") final String mapreduceId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      JsonArray result = getRunnableHistory(accountId, appId, mapreduceId);
      responder.sendJson(HttpResponseStatus.OK, result);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }


  private JsonArray getRunnableHistory(String accountId, String appId, String runnableid) throws Exception {
    TProtocol protocol =  getThriftProtocol(Services.APP_FABRIC, endpointStrategy);
    AppFabricService.Client client = new AppFabricService.Client(protocol);
    try {
      List<FlowRunRecord> records = client.getHistory(new FlowIdentifier(accountId, appId, runnableid, 1));
      JsonArray history = new JsonArray();
      for (FlowRunRecord record : records) {
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
