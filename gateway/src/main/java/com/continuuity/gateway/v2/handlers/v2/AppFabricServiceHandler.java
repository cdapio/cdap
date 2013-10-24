package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.services.AppFabricServiceException;
import com.continuuity.app.services.ArchiveId;
import com.continuuity.app.services.ArchiveInfo;
import com.continuuity.app.services.AuthToken;
import com.continuuity.app.services.DataType;
import com.continuuity.app.services.DeployStatus;
import com.continuuity.app.services.DeploymentStatus;
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
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.common.service.ServerException;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.util.ThriftHelper;
import com.continuuity.internal.app.WorkflowActionSpecificationCodec;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.commons.io.IOUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
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
@Path(Constants.Gateway.GATEWAY_VERSION)
public class AppFabricServiceHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AppFabricServiceHandler.class);
  private static final String ARCHIVE_NAME_HEADER = "X-Archive-Name";

  // For decoding runtime arguments in the start command.
  private static final Gson GSON =  new GsonBuilder()
                                            .registerTypeAdapter(WorkflowActionSpecification.class,
                                                                 new WorkflowActionSpecificationCodec())
                                            .create();
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() {}.getType();

  private final DiscoveryServiceClient discoveryClient;
  private final CConfiguration conf;
  private EndpointStrategy endpointStrategy;
  private final WorkflowClient workflowClient;
  private final QueueAdmin queueAdmin;

  @Inject
  public AppFabricServiceHandler(GatewayAuthenticator authenticator, CConfiguration conf,
                                 DiscoveryServiceClient discoveryClient, WorkflowClient workflowClient, QueueAdmin
    queueAdmin) {
    super(authenticator);
    this.discoveryClient = discoveryClient;
    this.conf = conf;
    this.workflowClient = workflowClient;
    this.queueAdmin = queueAdmin;
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
  @POST
  @Path("/apps")
  public void deploy(HttpRequest request, HttpResponder responder) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      String archiveName = request.getHeader(ARCHIVE_NAME_HEADER);

      if (archiveName == null || archiveName.isEmpty()) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, ARCHIVE_NAME_HEADER + " header not present");
        return;
      }

      ChannelBuffer content = request.getContent();
      if (content == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Archive is null");
        return;
      }

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
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
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Defines the class for sending deploy status to client.
   */
  private static class Status {
    private final int code;
    private final String status;
    private final String message;

    public Status(int code, String message) {
      this.code = code;
      this.status = DeployStatus.getMessage(code);
      this.message = message;
    }
  }

  /**
   * Gets application deployment status.
   */
  @GET
  @Path("/deploy/status")
  public void getDeployStatus(HttpRequest request, HttpResponder responder) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        DeploymentStatus status  = client.dstatus(token, new ArchiveId(accountId, "", ""));
        responder.sendJson(HttpResponseStatus.OK, new Status(status.getOverall(), status.getMessage()));
      } finally {
        if (client.getInputProtocol().getTransport().isOpen()) {
          client.getInputProtocol().getTransport().close();
        }
        if (client.getOutputProtocol().getTransport().isOpen()) {
          client.getOutputProtocol().getTransport().close();
        }
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
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
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Deletes an application specified by appId.
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
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
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
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
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
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns flow run history.
   */
  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/history")
  public void flowHistory(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId, @PathParam("flow-id") final String flowId) {
    QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
    String startTs = getQueryParameter(decoder.getParameters(), Constants.Gateway.QUERY_PARAM_START_TIME);
    String endTs = getQueryParameter(decoder.getParameters(), Constants.Gateway.QUERY_PARAM_END_TIME);
    String resultLimit = getQueryParameter(decoder.getParameters(), Constants.Gateway.QUERY_PARAM_LIMIT);

    long start = startTs == null ? Long.MIN_VALUE : Long.parseLong(startTs);
    long end = endTs == null ? Long.MAX_VALUE : Long.parseLong(endTs);
    int limit = resultLimit == null ? Constants.Gateway.DEFAULT_HISTORY_RESULTS_LIMIT : Integer.parseInt(resultLimit);
    getHistory(request, responder, appId, flowId, start, end, limit);
  }
  /**
   * Returns procedure run history.
   */
  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}/history")
  public void procedureHistory(HttpRequest request, HttpResponder responder,
                          @PathParam("app-id") final String appId,
                          @PathParam("procedure-id") final String procedureId) {
    QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
    String startTs = getQueryParameter(decoder.getParameters(), Constants.Gateway.QUERY_PARAM_START_TIME);
    String endTs = getQueryParameter(decoder.getParameters(), Constants.Gateway.QUERY_PARAM_END_TIME);
    String resultLimit = getQueryParameter(decoder.getParameters(), Constants.Gateway.QUERY_PARAM_LIMIT);

    long start = startTs == null ? Long.MIN_VALUE : Long.parseLong(startTs);
    long end = endTs == null ? Long.MAX_VALUE : Long.parseLong(endTs);
    int limit = resultLimit == null ? Constants.Gateway.DEFAULT_HISTORY_RESULTS_LIMIT : Integer.parseInt(resultLimit);

    getHistory(request, responder, appId, procedureId, start, end, limit);
  }

  /**
   * Returns mapreduce run history.
   */
  @GET
  @Path("/apps/{app-id}/mapreduce/{mapreduce-id}/history")
  public void mapreduceHistory(HttpRequest request, HttpResponder responder,
                          @PathParam("app-id") final String appId,
                          @PathParam("mapreduce-id") final String mapreduceId) {
    QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
    String startTs = getQueryParameter(decoder.getParameters(), Constants.Gateway.QUERY_PARAM_START_TIME);
    String endTs = getQueryParameter(decoder.getParameters(), Constants.Gateway.QUERY_PARAM_END_TIME);
    String resultLimit = getQueryParameter(decoder.getParameters(), Constants.Gateway.QUERY_PARAM_LIMIT);

    long start = startTs == null ? Long.MIN_VALUE : Long.parseLong(startTs);
    long end = endTs == null ? Long.MAX_VALUE : Long.parseLong(endTs);
    int limit = resultLimit == null ? Constants.Gateway.DEFAULT_HISTORY_RESULTS_LIMIT : Integer.parseInt(resultLimit);

    getHistory(request, responder, appId, mapreduceId, start, end, limit);
  }

  /**
   * Returns workflow run history.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/history")
  public void workflowHistory(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId,
                              @PathParam("workflow-id") final String workflowId) {
    QueryStringDecoder decoder = new QueryStringDecoder(request.getUri());
    String startTs = getQueryParameter(decoder.getParameters(), Constants.Gateway.QUERY_PARAM_START_TIME);
    String endTs = getQueryParameter(decoder.getParameters(), Constants.Gateway.QUERY_PARAM_END_TIME);
    String resultLimit = getQueryParameter(decoder.getParameters(), Constants.Gateway.QUERY_PARAM_LIMIT);

    long start = startTs == null ? Long.MIN_VALUE : Long.parseLong(startTs);
    long end = endTs == null ? Long.MAX_VALUE : Long.parseLong(endTs);
    int limit = resultLimit == null ? Constants.Gateway.DEFAULT_HISTORY_RESULTS_LIMIT : Integer.parseInt(resultLimit);

    getHistory(request, responder, appId, workflowId, start, end, limit);
  }

  private void getHistory(HttpRequest request, HttpResponder responder, String appId,
                          String id, long start, long end, int limit) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        List<ProgramRunRecord> records = client.getHistory(new ProgramId(accountId, appId, id), start, end, limit);
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
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        int count = client.getFlowletInstances(token, new ProgramId(accountId, appId, flowId), flowletId);
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
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Increases number of instance for a flowlet within a flow.
   */
  @PUT
  @Path("/apps/{app-id}/flows/{flow-id}/flowlets/{flowlet-id}/instances")
  public void setFlowletInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") final String appId, @PathParam("flow-id") final String flowId,
                                  @PathParam("flowlet-id") final String flowletId) {
    Short instances = 0;
    try {
      instances = getInstances(request);
      if (instances < 1) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Instance count should be greater than 0");
        return;
      }
    } catch (Throwable th) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid instance count.");
      return;
    }

    try {
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        client.setFlowletInstances(token, new ProgramId(accountId, appId, flowId), flowletId, instances);
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
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
   * Returns number of instances for a procedure.
   */
  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}/instances")
  public void getProcedureInstances(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") final String appId,
                                  @PathParam("procedure-id") final String procedureId) {

    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(procedureId);
    id.setType(EntityType.PROCEDURE);
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);

    try {
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      int count = client.getProgramInstances(token, id);
      JsonObject json = new JsonObject();
      json.addProperty("instances", count);

      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (Throwable throwable) {
      LOG.error("Got exception:", throwable);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }


  /**
   * Sets number of instances for a procedure
   */
  @PUT
  @Path("/apps/{app-id}/procedures/{procedure-id}/instances")
  public void setProcedureInstances(HttpRequest request, HttpResponder responder,
                                    @PathParam("app-id") final String appId,
                                    @PathParam("procedure-id") final String procedureId) {

    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(procedureId);
    id.setType(EntityType.PROCEDURE);
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);

    Short instances = 0;
    try {
      instances = getInstances(request);
      if (instances < 1) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Instance count should be greater than 0");
        return;
      }
    } catch (Throwable th) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST, "Invalid instance count.");
      return;
    }

    try {
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      client.setProgramInstances(token, id, instances);

      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Throwable throwable) {
      LOG.error("Got exception:", throwable);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private short getInstances(HttpRequest request) throws IOException, NumberFormatException {
    String instanceCount = "";
    Map<String, String> arguments = decodeArguments(request);
    if (!arguments.isEmpty()) {
      instanceCount = arguments.get("instances");
    }
    return Short.parseShort(instanceCount);
  }

  /**
   * Starts a mapreduce.
   */
  @POST
  @Path("/apps/{app-id}/mapreduce/{mapreduce-id}/start")
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
   * Starts a webapp.
   */
  @POST
  @Path("/apps/{app-id}/webapp/start")
  public void startWebapp(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") final String appId) {
    try {
      ProgramId id = new ProgramId();
      id.setApplicationId(appId);
      id.setFlowId(EntityType.WEBAPP.name().toLowerCase());
      id.setType(EntityType.WEBAPP);
      runnableStartStop(request, responder, id, "start");
    } catch (Throwable t) {
      LOG.error("Got exception:", t);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Save workflow runtime args.
   */
  @PUT
  @Path("/apps/{app-id}/workflows/{workflow-id}/runtimeargs")
  public void saveWorkflowRuntimeArgs(HttpRequest request, HttpResponder responder,
                            @PathParam("app-id") final String appId,
                            @PathParam("workflow-id") final String workflowId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(workflowId);
    id.setType(EntityType.WORKFLOW);
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);

    try {
      Map<String, String> args = decodeArguments(request);

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      client.storeRuntimeArguments(token, id, args);

      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get workflow runtime args.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/runtimeargs")
  public void getWorkflowRuntimeArgs(HttpRequest request, HttpResponder responder,
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
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      responder.sendJson(HttpResponseStatus.OK, client.getRuntimeArguments(token, id));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }


  /**
   * Save flow runtime args.
   */
  @PUT
  @Path("/apps/{app-id}/flows/{flow-id}/runtimeargs")
  public void saveFlowRuntimeArgs(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") final String appId,
                                      @PathParam("flow-id") final String flow) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(flow);
    id.setType(EntityType.FLOW);
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);

    try {
      Map<String, String> args = decodeArguments(request);

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      client.storeRuntimeArguments(token, id, args);

      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get flow runtime args.
   */
  @GET
  @Path("/apps/{app-id}/flows/{flow-id}/runtimeargs")
  public void getFlowRuntimeArgs(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId,
                                     @PathParam("flow-id") final String flowId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(flowId);
    id.setType(EntityType.FLOW);
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);

    try {
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      responder.sendJson(HttpResponseStatus.OK, client.getRuntimeArguments(token, id));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Save procedures runtime args.
   */
  @PUT
  @Path("/apps/{app-id}/procedures/{procedure-id}/runtimeargs")
  public void saveProcedureRuntimeArgs(HttpRequest request, HttpResponder responder,
                                  @PathParam("app-id") final String appId,
                                  @PathParam("procedure-id") final String procedureId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(procedureId);
    id.setType(EntityType.PROCEDURE);
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);

    try {
      Map<String, String> args = decodeArguments(request);

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      client.storeRuntimeArguments(token, id, args);

      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get procedures runtime args.
   */
  @GET
  @Path("/apps/{app-id}/procedures/{procedure-id}/runtimeargs")
  public void getProcedureRuntimeArgs(HttpRequest request, HttpResponder responder,
                                 @PathParam("app-id") final String appId,
                                 @PathParam("procedure-id") final String procedureId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(procedureId);
    id.setType(EntityType.PROCEDURE);
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);

    try {
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      responder.sendJson(HttpResponseStatus.OK, client.getRuntimeArguments(token, id));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }


  /**
   * Save mapreduce runtime args.
   */
  @PUT
  @Path("/apps/{app-id}/mapreduce/{mapreduce-id}/runtimeargs")
  public void saveMapReduceRuntimeArgs(HttpRequest request, HttpResponder responder,
                                       @PathParam("app-id") final String appId,
                                       @PathParam("mapreduce-id") final String mapreduceId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(mapreduceId);
    id.setType(EntityType.MAPREDUCE);
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);

    try {
      Map<String, String> args = decodeArguments(request);

      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      client.storeRuntimeArguments(token, id, args);

      responder.sendStatus(HttpResponseStatus.OK);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get mapreduce runtime args.
   */
  @GET
  @Path("/apps/{app-id}/mapreduce/{mapreduce-id}/runtimeargs")
  public void getMapReduceRuntimeArgs(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") final String appId,
                                      @PathParam("mapreduce-id") final String mapreduceId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(mapreduceId);
    id.setType(EntityType.MAPREDUCE);
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);

    try {
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      responder.sendJson(HttpResponseStatus.OK, client.getRuntimeArguments(token, id));
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
  @Path("/apps/{app-id}/mapreduce/{mapreduce-id}/stop")
  public void stopMapReduce(HttpRequest request, HttpResponder responder,
                             @PathParam("app-id") final String appId,
                             @PathParam("mapreduce-id") final String mapreduceId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(mapreduceId);
    id.setType(EntityType.MAPREDUCE);
    runnableStartStop(request, responder, id, "stop");
  }

  /**
   * Stops a webapp.
   */
  @POST
  @Path("/apps/{app-id}/webapp/stop")
  public void stopWebapp(HttpRequest request, HttpResponder responder,
                         @PathParam("app-id") final String appId) {
    try {
      ProgramId id = new ProgramId();
      id.setApplicationId(appId);
      id.setFlowId(EntityType.WEBAPP.name().toLowerCase());
      id.setType(EntityType.WEBAPP);
      runnableStartStop(request, responder, id, "stop");
    } catch (Throwable t) {
      LOG.error("Got exception:", t);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void runnableStartStop(HttpRequest request, HttpResponder responder,
                                 ProgramId id, String action) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      id.setAccountId(accountId);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        if ("start".equals(action)) {
          client.start(token, new ProgramDescriptor(id, decodeArguments(request)));
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
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private Map<String, String> decodeArguments(HttpRequest request) throws IOException {
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
  @DELETE
  @Path("/apps/{app-id}/flows/{flow-id}/queues")
  public void deleteFlowQueues(HttpRequest request, HttpResponder responder,
                               @PathParam("app-id") final String appId,
                               @PathParam("flow-id") final String flowId) {

    String accountId = getAuthenticatedAccountId(request);
    ProgramId id = new ProgramId();
    id.setAccountId(accountId);
    id.setApplicationId(appId);
    id.setFlowId(flowId);
    id.setType(EntityType.FLOW);
    try {
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      ProgramStatus status = getProgramStatus(token, id);
      if (status.getStatus().equals("NOT_FOUND")) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else if (status.getStatus().equals("RUNNING")) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, "Flow is running, please stop it first.");
      } else {
        queueAdmin.dropAllForFlow(appId, flowId);
        deleteMetricsForFlow(appId, flowId);
        responder.sendStatus(HttpResponseStatus.OK);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private void deleteMetricsForFlow(String application, String flow) throws IOException {
    Iterable<Discoverable> discoverables = this.discoveryClient.discover(Constants.Service.GATEWAY);
    Discoverable discoverable = new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoverables),
                                                              3L, TimeUnit.SECONDS).pick();

    if (discoverable == null) {
      LOG.error("Fail to get any metrics endpoint for deleting metrics.");
      return;
    }

    LOG.debug("Deleting metrics for flow {}.{}", application, flow);
    for (MetricsScope scope : MetricsScope.values()) {
      String url = String.format("http://%s:%d%s/metrics/%s/apps/%s/flows/%s",
                                 discoverable.getSocketAddress().getHostName(),
                                 discoverable.getSocketAddress().getPort(),
                                 Constants.Gateway.GATEWAY_VERSION,
                                 scope.name().toLowerCase(),
                                 application, flow);

      long timeout = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

      SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
        .setUrl(url)
        .setRequestTimeoutInMs((int) timeout)
        .build();

      try {
        client.delete().get(timeout, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        LOG.error("exception making metrics delete call", e);
        Throwables.propagate(e);
      } finally {
        client.close();
      }
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
  @Path("/apps/{app-id}/mapreduce/{mapreduce-id}/status")
  public void mapreduceStatus(final HttpRequest request, final HttpResponder responder,
                              @PathParam("app-id") final String appId,
                              @PathParam("mapreduce-id") final String mapreduceId) {

    // Get the runnable status
    // If runnable is not running
    //   - Get the status from workflow
    //
    AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));

    String accountId = getAuthenticatedAccountId(request);

    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(mapreduceId);
    id.setType(EntityType.MAPREDUCE);
    id.setAccountId(accountId);

    try {

      ProgramStatus status = getProgramStatus(token, id);
      if (status.getStatus().equals("NOT_FOUND")) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else if (!status.getStatus().equals("RUNNING")) {
        //Program status is not running, check if it is running as a part of workflow
        String workflowName = getWorkflowName(id.getFlowId());
        workflowClient.getWorkflowStatus(id.getAccountId(), id.getApplicationId(), workflowName,
                                         new WorkflowClient.Callback() {
                                           @Override
                                           public void handle(WorkflowClient.Status status) {
                                             JsonObject o = new JsonObject();
                                             if (status.getCode().equals(WorkflowClient.Status.Code.OK)) {
                                               o.addProperty("status", "RUNNING");
                                             } else {
                                               o.addProperty("status", "STOPPED");
                                             }
                                             responder.sendJson(HttpResponseStatus.OK, o);
                                           }
                                         });
      } else {
        JsonObject o = new JsonObject();
        o.addProperty("status", status.getStatus());
        responder.sendJson(HttpResponseStatus.OK, o);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get workflow name from mapreduceId.
   * Format of mapreduceId: WorkflowName_mapreduceName, if the mapreduce is a part of workflow.
   *
   * @param mapreduceId id of the mapreduce job in reactor.
   * @return workflow name if exists null otherwise
   */
  private String getWorkflowName(String mapreduceId) {
    String [] splits = mapreduceId.split("_");
    if (splits.length > 1) {
      return splits[0];
    } else {
      return null;
    }
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

  /**
   * Returns status of a webapp.
   */
  @GET
  @Path("/apps/{app-id}/webapp/status")
  public void webappStatus(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId) {
    try {
      ProgramId id = new ProgramId();
      id.setApplicationId(appId);
      id.setFlowId(EntityType.WEBAPP.name().toLowerCase());
      id.setType(EntityType.WEBAPP);
      runnableStatus(request, responder, id);
    } catch (Throwable t) {
      LOG.error("Got exception:", t);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }


  private void runnableStatus(HttpRequest request, HttpResponder responder, ProgramId id) {
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);
    try {
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      ProgramStatus status = getProgramStatus(token, id);
      if (status.getStatus().equals("NOT_FOUND")){
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else {
        JsonObject o = new JsonObject();
        o.addProperty("status", status.getStatus());
        responder.sendJson(HttpResponseStatus.OK, o);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private ProgramStatus getProgramStatus(AuthToken token, ProgramId id)
    throws ServerException, TException, AppFabricServiceException {

    TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
    AppFabricService.Client client = new AppFabricService.Client(protocol);
    try {
      return client.status(token, id);
    } finally {
      if (client.getInputProtocol().getTransport().isOpen()) {
        client.getInputProtocol().getTransport().close();
      }
      if (client.getOutputProtocol().getTransport().isOpen()) {
        client.getOutputProtocol().getTransport().close();
      }
    }
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
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      List<ScheduleRunTime> runtimes = client.getNextScheduledRunTime(token, id);
      JsonArray array = new JsonArray();
      for (ScheduleRunTime runtime : runtimes){
        JsonObject object = new JsonObject();
        object.addProperty("id", runtime.getId().getId());
        object.addProperty("time", runtime.getTime());
        array.add(object);
      }
      responder.sendJson(HttpResponseStatus.OK, array);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      List<ScheduleId> scheduleIds = client.getSchedules(token, id);
      List<String> schedules =  Lists.newArrayList(Lists.transform(scheduleIds,
                                                                   new Function<ScheduleId, String>() {
                                                                     @Override
                                                                     public String apply(ScheduleId id) {
                                                                       return id.getId();
                                                                     }
                                                                   }));
      responder.sendJson(HttpResponseStatus.OK, schedules);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Get schedule state.
   */
  @GET
  @Path("/apps/{app-id}/workflows/{workflow-id}/schedules/{schedule-id}/status")
  public void getScheuleState(HttpRequest request, HttpResponder responder,
                                @PathParam("app-id") final String appId,
                                @PathParam("workflow-id") final String workflowId,
                                @PathParam("schedule-id") final String scheduleId) {
    try {
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      String schedule = client.getScheduleState(new ScheduleId(scheduleId));
      JsonObject json = new JsonObject();
      json.addProperty("status", schedule);
      responder.sendJson(HttpResponseStatus.OK, json);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      client.suspendSchedule(token, new ScheduleId(scheduleId));
      responder.sendJson(HttpResponseStatus.OK, "OK");
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
      TProtocol protocol = ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      client.resumeSchedule(token, new ScheduleId(scheduleId));
      responder.sendJson(HttpResponseStatus.OK, "OK");
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
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
    getProgramById(request, responder, id);
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
    getProgramById(request, responder, id);
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
    getProgramById(request, responder, id);
  }

  /**
   * Returns specification of a mapreduce.
   */
  @GET
  @Path("/apps/{app-id}/mapreduce/{mapreduce-id}")
  public void mapreduceSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("app-id") final String appId,
                                     @PathParam("mapreduce-id") final String mapreduceId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setFlowId(mapreduceId);
    id.setType(EntityType.MAPREDUCE);
    getProgramById(request, responder, id);
  }

  private void getProgramById(HttpRequest request, HttpResponder responder, ProgramId id) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      id.setAccountId(accountId);
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        String specification = client.getSpecification(id);
        if (specification.isEmpty()) {
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        } else {
          responder.sendByteArray(HttpResponseStatus.OK, specification.getBytes(Charsets.UTF_8),
                                  ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
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
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns a list of flows associated with account.
   */
  @GET
  @Path("/flows")
  public void getAllFlows(HttpRequest request, HttpResponder responder) {
    programList(request, responder, EntityType.FLOW, null);
  }

  /**
   * Returns a list of procedures associated with account.
   */
  @GET
  @Path("/procedures")
  public void getAllProcedures(HttpRequest request, HttpResponder responder) {
    programList(request, responder, EntityType.PROCEDURE, null);
  }

  /**
   * Returns a list of map/reduces associated with account.
   */
  @GET
  @Path("/mapreduce")
  public void getAllMapReduce(HttpRequest request, HttpResponder responder) {
    programList(request, responder, EntityType.MAPREDUCE, null);
  }

  /**
   * Returns a list of workflows associated with account.
   */
  @GET
  @Path("/workflows")
  public void getAllWorkflows(HttpRequest request, HttpResponder responder) {
    programList(request, responder, EntityType.WORKFLOW, null);
  }

  /**
   * Returns a list of applications associated with account.
   */
  @GET
  @Path("/apps")
  public void getAllApps(HttpRequest request, HttpResponder responder) {
    programList(request, responder, EntityType.APP, null);
  }

  /**
   * Returns a list of applications associated with account.
   */
  @GET
  @Path("/apps/{app-id}")
  public void getApps(HttpRequest request, HttpResponder responder,
                      @PathParam("app-id") final String appId) {
    ProgramId id = new ProgramId();
    id.setApplicationId(appId);
    id.setType(EntityType.APP);
    id.setFlowId("");
    getProgramById(request, responder, id);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/flows")
  public void getFlowsByApp(HttpRequest request, HttpResponder responder,
                                 @PathParam("app-id") final String appId) {
    programList(request, responder, EntityType.FLOW, appId);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/procedures")
  public void getProceduresByApp(HttpRequest request, HttpResponder responder,
                                 @PathParam("app-id") final String appId) {
    programList(request, responder, EntityType.PROCEDURE, appId);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/mapreduce")
  public void getMapreduceByApp(HttpRequest request, HttpResponder responder,
                                 @PathParam("app-id") final String appId) {
    programList(request, responder, EntityType.MAPREDUCE, appId);
  }

  /**
   * Returns a list of procedure associated with account & application.
   */
  @GET
  @Path("/apps/{app-id}/workflows")
  public void getWorkflowssByApp(HttpRequest request, HttpResponder responder,
                                 @PathParam("app-id") final String appId) {
    programList(request, responder, EntityType.WORKFLOW, appId);
  }

  private void programList(HttpRequest request, HttpResponder responder, EntityType type, String appid) {
    try {
      if (appid != null && appid.isEmpty()) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "app-id is null or empty");
        return;
      }
      String accountId = getAuthenticatedAccountId(request);
      ProgramId id = new ProgramId(accountId, appid == null ? "" : appid, ""); // no program
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        String list = appid == null ? client.listPrograms(id, type) : client.listProgramsByApp(id, type);
        if (list.isEmpty()) {
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        } else {
          responder.sendByteArray(HttpResponseStatus.OK, list.getBytes(Charsets.UTF_8),
                                  ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
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
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns all flows associated with a stream.
   */
  @GET
  @Path("/streams/{stream-id}/flows")
  public void getFlowsByStream(HttpRequest request, HttpResponder responder,
                               @PathParam("stream-id") final String streamId) {
    programListByDataAccess(request, responder, EntityType.FLOW, DataType.STREAM, streamId);
  }

  /**
   * Returns all flows associated with a dataset.
   */
  @GET
  @Path("/datasets/{dataset-id}/flows")
  public void getFlowsByDataset(HttpRequest request, HttpResponder responder,
                                @PathParam("dataset-id") final String datasetId) {
    programListByDataAccess(request, responder, EntityType.FLOW, DataType.DATASET, datasetId);
  }

  private void programListByDataAccess(HttpRequest request, HttpResponder responder, EntityType type,
                                       DataType datatype, String name) {
    try {
      if (name.isEmpty()) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, datatype.name().toLowerCase() + " name is empty");
        return;
      }
      String accountId = getAuthenticatedAccountId(request);
      ProgramId id = new ProgramId(accountId, "", ""); // no app, no program
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        String list = client.listProgramsByDataAccess(id, type, datatype, name);
        if (list.isEmpty()) {
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        } else {
          responder.sendByteArray(HttpResponseStatus.OK, list.getBytes(Charsets.UTF_8),
                                  ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
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
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Returns a list of streams associated with account.
   */
  @GET
  @Path("/streams")
  public void getStreams(HttpRequest request, HttpResponder responder) {
    dataList(request, responder, DataType.STREAM, null, null);
  }

  /**
   * Returns a stream associated with account.
   */
  @GET
  @Path("/streams/{stream-id}")
  public void getStreamSpecification(HttpRequest request, HttpResponder responder,
                                     @PathParam("stream-id") final String streamId) {
    dataList(request, responder, DataType.STREAM, streamId, null);
  }

  /**
   * Returns a list of streams associated with application.
   */
  @GET
  @Path("/apps/{app-id}/streams")
  public void getStreamsByApp(HttpRequest request, HttpResponder responder,
                              @PathParam("app-id") final String appId) {
    dataList(request, responder, DataType.STREAM, null, appId);
  }

  /**
   * Returns a list of dataset associated with account.
   */
  @GET
  @Path("/datasets")
  public void getDatasets(HttpRequest request, HttpResponder responder) {
    dataList(request, responder, DataType.DATASET, null, null);
  }

  /**
   * Returns a dataset associated with account.
   */
  @GET
  @Path("/datasets/{dataset-id}")
  public void getDatasetSpecification(HttpRequest request, HttpResponder responder,
                                      @PathParam("dataset-id") final String datasetId) {
    dataList(request, responder, DataType.DATASET, datasetId, null);
  }

  /**
   * Returns a list of dataset associated with application.
   */
  @GET
  @Path("/apps/{app-id}/datasets")
  public void getDatasetsByApp(HttpRequest request, HttpResponder responder,
                               @PathParam("app-id") final String appId) {
    dataList(request, responder, DataType.DATASET, null, appId);
  }

  private void dataList(HttpRequest request, HttpResponder responder, DataType type, String name, String app) {
    try {
      if ((name != null && name.isEmpty()) || (app != null && app.isEmpty())) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Empty name provided.");
        return;
      }
      String accountId = getAuthenticatedAccountId(request);
      ProgramId id = new ProgramId(accountId, app == null ? "" : app, ""); // no program
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);
      try {
        String json = name != null ? client.getDataEntity(id, type, name) :
          app != null ? client.listDataEntitiesByApp(id, type) : client.listDataEntities(id, type);
        if (json.isEmpty()) {
          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
        } else {
          responder.sendByteArray(HttpResponseStatus.OK, json.getBytes(Charsets.UTF_8),
                                  ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
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
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * *DO NOT DOCUMENT THIS API*
   */
  @POST
  @Path("/unrecoverable/reset")
  public void resetReactor(HttpRequest request, HttpResponder responder) {
    try {
      if (!conf.getBoolean(Constants.Dangerous.UNRECOVERABLE_RESET, Constants.Dangerous.DEFAULT_UNRECOVERABLE_RESET)) {
        responder.sendStatus(HttpResponseStatus.FORBIDDEN);
        return;
      }
      String accountId = getAuthenticatedAccountId(request);
      AuthToken token = new AuthToken(request.getHeader(GatewayAuthenticator.CONTINUUITY_API_KEY));
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
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
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  private String getQueryParameter(Map<String, List<String>> parameters, String parameterName) {
    if (parameters == null || parameters.isEmpty()) {
      return null;
    } else {
      List<String> matchedParams = parameters.get(parameterName);
      return matchedParams == null || matchedParams.isEmpty() ? null : matchedParams.get(0);
    }
  }
}
