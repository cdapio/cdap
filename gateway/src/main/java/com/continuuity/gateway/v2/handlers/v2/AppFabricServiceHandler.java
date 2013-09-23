package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AppFabricServiceException;
import com.continuuity.app.services.ArchiveId;
import com.continuuity.app.services.ArchiveInfo;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.EntityType;
import com.continuuity.app.services.ProgramDescriptor;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.ProgramRunRecord;
import com.continuuity.app.services.ProgramStatus;
import com.continuuity.app.services.ScheduleId;
import com.continuuity.app.services.ScheduleRunTime;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.thrift.protocol.TProtocol;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *  {@link AppFabricServiceHandler} is REST interface to AppFabric backend.
 */
@Path("/v2")
public class AppFabricServiceHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServiceHandler.class);
  private static final String ARCHIVE_NAME_HEADER = "X-Archive-Name";

  // For decoding runtime arguments in the start command.
  private static final Gson GSON = new Gson();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() {}.getType();

  private final DiscoveryServiceClient discoveryClient;
  private final CConfiguration conf;
  private EndpointStrategy endpointStrategy;

  @Inject
  public AppFabricServiceHandler(GatewayAuthenticator authenticator, CConfiguration conf,
                                 DiscoveryServiceClient discoveryClient) {
    super(authenticator);
    this.discoveryClient = discoveryClient;
    this.conf = conf;
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    this.endpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.APP_FABRIC)),
      1L, TimeUnit.SECONDS);
  }

  /**
   * Deploys an application.
   */
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
      TProtocol protocol =  getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      try {
        ArchiveInfo rInfo = new ArchiveInfo(accountId, "gateway", archiveName);
        ArchiveId rIdentifier = client.init(token, rInfo);

        while (content.readableBytes() > 0) {
          int bytesToRead = Math.min(1024 * 1024, content.readableBytes());
          client.chunk(token, rIdentifier, content.readSlice(bytesToRead).toByteBuffer());
        }

        client.deploy(token, rIdentifier);
        responder.sendStatus(HttpResponseStatus.OK);
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Deletes an application specified by appId
   */
  @DELETE
  @Path("/apps/{app-id}")
  public void deleteApp(HttpRequest request, HttpResponder responder,
                        @PathParam("app-id") final String appId) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
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
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Deletes an application specified by appId
   */
  @POST
  @Path("/apps/{app-id}/promote")
  public void promoteApp(HttpRequest request, HttpResponder responder, @PathParam("app-id") final String appId) {
    try {
      String postBody = null;

      try {
        postBody = IOUtils.toString(new ChannelBufferInputStream(request.getContent()));
      } catch (IOException e) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST, e.getMessage());
        return;
      }

      Map<String, String> o = null;
      try {
        o = GSON.fromJson(postBody, MAP_STRING_STRING_TYPE);
      } catch (JsonSyntaxException e) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST, "Not a valid body specified.");
        return;
      }

      if (!o.containsKey("hostname")) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST, "Hostname not specified.");
        return;
      }

      // Checks DNS, Ipv4, Ipv6 address in one go.
      try {
        InetAddress address = InetAddress.getByName(o.get("hostname"));
      } catch (UnknownHostException e) {
        responder.sendError(HttpResponseStatus.BAD_REQUEST, "Unknown hostname.");
        return;
      }

      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        try {
          if (!client.promote(token,
                         new ArchiveId(accountId, appId, "promote-" + System.currentTimeMillis() + ".jar"),
                         o.get("hostname"))) {
            responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Failed to promote application " + appId);
          } else {
            responder.sendStatus(HttpResponseStatus.OK);
          }
        } catch (AppFabricServiceException e) {
          responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
          return;
        }

      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
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
      TProtocol protocol =  getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
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
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Returns flow run history.
   */
  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/history")
  public void flowHistory(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId, @PathParam("flow-id") final String flowId) {
    getHistory(request, responder, appId, flowId);

  }
  /**
   * Returns procedure run history.
   */
  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}/history")
  public void procedureHistory(HttpRequest request, HttpResponder responder,
                          @PathParam("app-id") final String appId,
                          @PathParam("procedure-id") final String procedureId) {
    getHistory(request, responder, appId, procedureId);

  }

  /**
   * Returns mapreduce run history.
   */
  @GET
  @Path("/apps/{app-id}/mapreduces/{mapreduce-id}/history")
  public void mapreduceHistory(HttpRequest request, HttpResponder responder,
                          @PathParam("app-id") final String appId,
                          @PathParam("mapreduce-id") final String mapreduceId) {
    getHistory(request, responder, appId, mapreduceId);

  }

  /**
   * Returns workflow run history.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/history")
  public void workflowHistory(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId,
                              @PathParam("workflow-id") final String workflowId) {
    getHistory(request, responder, appId, workflowId);
  }

  private void getHistory(HttpRequest request, HttpResponder responder, String appId, String id) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      TProtocol protocol =  getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        List<ProgramRunRecord> records = client.getHistory(new ProgramId(accountId, appId, id));
        JsonArray history = new JsonArray();
        for (ProgramRunRecord record : records) {
          JsonObject object = new JsonObject();
          object.addProperty("runid", record.getRunId());
          object.addProperty("start", record.getStartTime());
          object.addProperty("end", record.getEndTime());
          object.addProperty("status", record.getEndStatus());
          history.add(object);
        }
        responder.sendJson(HttpResponseStatus.OK, history);
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Promote an application another reactor.
   */


  /**
   * Returns number of instances for a flowlet within a flow.
   */
  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/instances")
  public void getFlowletInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") final String appId, @PathParam("flow-id") final String flowId,
                                  @PathParam("flowlet-id") final String flowletId) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
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
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Increases number of instance for a flowlet within a flow.
   */
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
      TProtocol protocol =  getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
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
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Starts a flow.
   */
  @POST
  @Path("/apps/{app-id}/flows/{flow-id}/start")
  public void startFlow(HttpRequest request, HttpResponder responder,
                        @PathParam("app-id") final String appId, @PathParam("flow-id") final String flowId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(flowId);
    id.setType(EntityType.FLOW);
    runnableStartStop(request, responder, id, "start");
  }

  /**
   * Starts a procedure.
   */
  @POST
  @Path("/apps/{app-id}/procedures/{procedure-id}/start")
  public void startProcedure(HttpRequest request, HttpResponder responder,
                            @PathParam("app-id") final String appId,
                            @PathParam("procedure-id") final String procedureId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(procedureId);
    id.setType(EntityType.PROCEDURE);
    runnableStartStop(request, responder, id, "start");
  }

  /**
   * Starts a mapreduce.
   */
  @POST
  @Path("/apps/{app-id}/mapreduces/{mapreduce-id}/start")
  public void startMapReduce(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") final String appId,
                             @PathParam("mapreduce-id") final String mapreduceId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(mapreduceId);
    id.setType(EntityType.MAPREDUCE);
    runnableStartStop(request, responder, id, "start");
  }

  /**
   * Starts a workflow.
   */
  @POST
  @Path("/apps/{app-id}/workflows/{workflow-id}/start")
  public void startWorkflow(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") final String appId,
                             @PathParam("workflow-id") final String workflowId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(workflowId);
    id.setType(EntityType.WORKFLOW);
    runnableStartStop(request, responder, id, "start");
  }


  /**
   * Saves a workflow spec.
   */
  @POST
  @Path("/apps/{app-id}/workflows/{workflow-id}/save")
  public void saveWorkflow(HttpRequest request, HttpResponder responder,
                            @PathParam("app-id") final String appId,
                            @PathParam("workflow-id") final String workflowId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(workflowId);
    id.setType(EntityType.WORKFLOW);
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);

    try {
      Map<String, String> args = decodeRuntimeArguments(request);

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      client.storeRuntimeArguments(token, id, args);
      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }

  }

  /**
   * Stops a flow.
   */
  @POST
  @Path("/apps/{app-id}/flows/{flow-id}/stop")
  public void stopFlow(HttpRequest request, HttpResponder responder,
                        @PathParam("app-id") final String appId, @PathParam("flow-id") final String flowId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(flowId);
    id.setType(EntityType.FLOW);
    runnableStartStop(request, responder, id, "stop");
  }

  /**
   * Stops a procedure.
   */
  @POST
  @Path("/apps/{app-id}/procedures/{procedure-id}/stop")
  public void stopProcedure(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") final String appId,
                             @PathParam("procedure-id") final String procedureId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(procedureId);
    id.setType(EntityType.PROCEDURE);
    runnableStartStop(request, responder, id, "stop");
  }

  /**
   * Stops a mapreduce.
   */
  @POST
  @Path("/apps/{app-id}/mapreduces/{mapreduce-id}/stop")
  public void stopMapReduce(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") final String appId,
                             @PathParam("mapreduce-id") final String mapreduceId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(mapreduceId);
    id.setType(EntityType.MAPREDUCE);
    runnableStartStop(request, responder, id, "stop");
  }


  private void runnableStartStop(HttpRequest request, HttpResponder responder,
                                 ProgramId id, String action) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      id.setAccountId(accountId);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        if ("start".equals(action)) {
          client.start(token, new ProgramDescriptor(id, decodeRuntimeArguments(request)));
        } else if ("stop".equals(action)) {
          client.stop(token, id);
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
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  private Map<String, String> decodeRuntimeArguments(HttpRequest request) throws IOException {
    ChannelBuffer content = request.getContent();
    if (!content.readable()) {
      return ImmutableMap.of();
    }
    Reader reader = new InputStreamReader(new ChannelBufferInputStream(content), Charsets.UTF_8);
    try {
      Map<String, String> args = GSON.fromJson(reader, MAP_STRING_STRING_TYPE);
      return args == null ? ImmutableMap.<String, String>of() : args;
    } catch (JsonSyntaxException e) {
      LOG.info("Failed to parse runtime arguments on {}", request.getUri(), e);
      throw e;
    } finally {
      reader.close();
    }
  }

  /**
   * Returns status of a flow.
   */
  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/status")
  public void flowStatus(HttpRequest request, HttpResponder responder,
                         @PathParam("app-id") final String appId,
                         @PathParam("flow-id") final String flowId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(flowId);
    id.setType(EntityType.FLOW);
    runnableStatus(request, responder, id);
  }

  /**
   * Returns status of a procedure.
   */
  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}/status")
  public void procedureStatus(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId,
                              @PathParam("procedure-id") final String procedureId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(procedureId);
    id.setType(EntityType.PROCEDURE);
    runnableStatus(request, responder, id);
  }

  /**
   * Returns status of a mapreduce.
   */
  @GET
  @Path("/apps/{app-id}/mapreduces/{mapreduce-id}/status")
  public void mapreduceStatus(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId,
                              @PathParam("mapreduce-id") final String mapreduceId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(mapreduceId);
    id.setType(EntityType.MAPREDUCE);
    runnableStatus(request, responder, id);
  }


  /**
   * Returns status of a workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/status")
  public void workflowStatus(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") final String appId,
                             @PathParam("workflow-id") final String workflowId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(workflowId);
    id.setType(EntityType.WORKFLOW);
    runnableStatus(request, responder, id);
  }


  private void runnableStatus(HttpRequest request, HttpResponder responder, ProgramId id) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      id.setAccountId(accountId);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        ProgramStatus status = client.status(token, id);
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
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Returns specification of a flow.
   */
  @GET
  @Path("/apps/{app-id}/flows/{flow-id}")
  public void flowSpecification(HttpRequest request, HttpResponder responder,
                                    @PathParam("app-id") final String appId,
                                    @PathParam("flow-id") final String flowId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(flowId);
    id.setType(EntityType.FLOW);
    runnableSpecification(request, responder, id);
  }

  /**
   * Returns specification of a procedure.
   */
  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}")
  public void procedureSpecification(HttpRequest request, HttpResponder responder,
                                    @PathParam("app-id") final String appId,
                                    @PathParam("procedure-id") final String procedureId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(procedureId);
    id.setType(EntityType.PROCEDURE);
    runnableSpecification(request, responder, id);
  }

  /**
   * Returns specification of a workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}")
  public void workflowSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId,
                                     @PathParam("workflow-id") final String workflowId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(workflowId);
    id.setType(EntityType.WORKFLOW);
    runnableSpecification(request, responder, id);
  }

  /**
   * Returns next scheduled runtime of a workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/nextruntime")
  public void getScheduledRunTime(HttpRequest request, HttpResponder responder,
                                    @PathParam("app-id") final String appId,
                                    @PathParam("workflow-id") final String workflowId) {

    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(workflowId);
    id.setType(EntityType.WORKFLOW);
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);

    try {

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      List<ScheduleRunTime> runtimes = client.getNextScheduledRunTime(token, id);
      responder.sendJson(HttpResponseStatus.OK, runtimes);
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Get list of schedules for a given workflow.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules")
  public void workflowSchedules(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId,
                                @PathParam("workflow-id") final String workflowId) {

    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(workflowId);
    id.setType(EntityType.WORKFLOW);
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);

    try {

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      List<ScheduleId> schedules = client.getSchedules(token, id);
      responder.sendJson(HttpResponseStatus.OK, schedules);
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Suspend a workflow schedule.
   */
  @POST
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules/{schedule-id}/suspend")
  public void workflowScheduleSuspend(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId,
                                @PathParam("workflow-id") final String workflowId,
                                @PathParam("schedule-id") final String scheduleId) {
    try {
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      client.suspendSchedule(token, new ScheduleId(scheduleId));
      responder.sendJson(HttpResponseStatus.OK, "OK");
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * Resume a workflow schedule.
   */
  @POST
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules/{schedule-id}/resume")
  public void workflowScheduleResume(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") final String appId,
                                      @PathParam("workflow-id") final String workflowId,
                                      @PathParam("schedule-id") final String scheduleId) {

    try {
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      client.resumeSchedule(token, new ScheduleId(scheduleId));
      responder.sendJson(HttpResponseStatus.OK, "OK");
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }


  /**
   * Returns specification of a mapreduce.
   */
  @GET
  @Path("/apps/{app-id}/mapreduces/{mapreduce-id}")
  public void mapreduceSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId,
                                     @PathParam("mapreduce-id") final String mapreduceId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(mapreduceId);
    id.setType(EntityType.MAPREDUCE);
    runnableSpecification(request, responder, id);
  }

  private void runnableSpecification(HttpRequest request, HttpResponder responder, ProgramId id) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      id.setAccountId(accountId);
      TProtocol protocol =  getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        String specification = client.getSpecification(id);
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
    } catch (SecurityException e) {
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

  /**
   * *DO NOT DOCUMENT THIS API*
   */
  @DELETE
  @Path("/unrecoverable/reset")
  public void resetReactor(HttpRequest request, HttpResponder responder) {
    try {
      if (!conf.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, Constants.Dangerous.DEFAULT_UNRECOVERABLE_RESET)) {
        responder.sendStatus(HttpResponseStatus.FORBIDDEN);
        return;
      }
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        client.reset(token, accountId);
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
      responder.sendString(HttpResponseStatus.FORBIDDEN, e.getMessage());
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }
  }

}
