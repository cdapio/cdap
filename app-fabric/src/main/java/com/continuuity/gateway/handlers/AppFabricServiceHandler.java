package com.continuuity.gateway.handlers;

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
import com.continuuity.app.services.ExceptionCode;
import com.continuuity.app.services.ProgramDescriptor;
import com.continuuity.app.services.ProgramId;
import com.continuuity.app.services.ProgramRunRecord;
import com.continuuity.app.services.ProgramStatus;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.service.ServerException;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.util.ThriftHelper;
import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.app.WorkflowActionSpecificationCodec;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
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
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
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
  private static final Type MAP_STRING_STRING_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final DiscoveryServiceClient discoveryClient;
  private final CConfiguration conf;
  private EndpointStrategy endpointStrategy;
  private EndpointStrategy httpEndpointStrategy;
  private final WorkflowClient workflowClient;
  private final QueueAdmin queueAdmin;

  @Inject
  public AppFabricServiceHandler(Authenticator authenticator, CConfiguration conf,
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

    this.httpEndpointStrategy = new TimeLimitEndpointStrategy(
      new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.APP_FABRIC_HTTP)),
      1L, TimeUnit.SECONDS);
  }

  /**
   * Deploys an application with speicifed name.
   */
  @PUT
  @Path("/apps/{app-id}")
  public void deploy(HttpRequest request, HttpResponder responder, @PathParam("app-id") final String appId) {
    deployApp(request, responder, appId);
  }

  /**
   * Deploys an application.
   */
  @POST
  @Path("/apps")
  public void deploy(HttpRequest request, HttpResponder responder) {
    // null means use name provided by app spec
    deployApp(request, responder, null);
  }

  private void deployApp(HttpRequest request, HttpResponder responder, @Nullable String appName) {
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

      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
      TProtocol protocol =  ThriftHelper.getThriftProtocol(Constants.Service.APP_FABRIC, endpointStrategy);
      AppFabricService.Client client = new AppFabricService.Client(protocol);

      try {
        ArchiveInfo rInfo = new ArchiveInfo(accountId, archiveName);
        rInfo.setApplicationId(appName);
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
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
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
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
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
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
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
   * Promote an application another reactor.
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
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
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
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
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
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
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
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
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
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
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
    } catch (AppFabricServiceException e) {
      if (e.getCode() == ExceptionCode.NOT_FOUND) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else if (e.getCode() == ExceptionCode.ILLEGAL_STATE) {
        responder.sendString(HttpResponseStatus.CONFLICT, e.getMessage());
      } else {
        LOG.error("Got exception:", e);
        responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
      }
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
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
      ProgramStatus status = getProgramStatus(token, id);
      if (status.getStatus().equals("NOT_FOUND")) {
        responder.sendStatus(HttpResponseStatus.NOT_FOUND);
      } else if (status.getStatus().equals("RUNNING")) {
        responder.sendString(HttpResponseStatus.FORBIDDEN, "Flow is running, please stop it first.");
      } else {
        queueAdmin.dropAllForFlow(appId, flowId);
        // delete process metrics that are used to calculate the queue size (process.events.pending metric name)
        deleteProcessMetricsForFlow(appId, flowId);
        responder.sendStatus(HttpResponseStatus.OK);
      }
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.UNAUTHORIZED);
    } catch (Throwable e) {
      LOG.error("Got exception:", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  // deletes the process metrics for a flow
  private void deleteProcessMetricsForFlow(String application, String flow) throws IOException {
    Iterable<Discoverable> discoverables = this.discoveryClient.discover(Constants.Service.GATEWAY);
    Discoverable discoverable = new TimeLimitEndpointStrategy(new RandomEndpointStrategy(discoverables),
                                                              3L, TimeUnit.SECONDS).pick();

    if (discoverable == null) {
      LOG.error("Fail to get any metrics endpoint for deleting metrics.");
      return;
    }

    LOG.debug("Deleting metrics for flow {}.{}", application, flow);
    String url = String.format("http://%s:%d%s/metrics/reactor/apps/%s/flows/%s?prefixEntity=process",
                               discoverable.getSocketAddress().getHostName(),
                               discoverable.getSocketAddress().getPort(),
                               Constants.Gateway.GATEWAY_VERSION,
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

 /*
  will remove this once webappStatus is ported to AppFabricHttpHandler
  */
  private void runnableStatus(HttpRequest request, HttpResponder responder, ProgramId id) {
    String accountId = getAuthenticatedAccountId(request);
    id.setAccountId(accountId);
    //id.setAccountId("default");
    try {
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
      ProgramStatus status = getProgramStatus(token, id);
      if (status.getStatus().equals("NOT_FOUND")) {
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
      LOG.info("Status call from AppFabricHttpHandler for app {}  webapp", appId);
    } catch (Throwable t) {
      LOG.error("Got exception:", t);
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
      AuthToken token = new AuthToken(request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY));
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
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
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
