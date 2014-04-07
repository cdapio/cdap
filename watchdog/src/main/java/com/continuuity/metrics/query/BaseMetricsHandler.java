/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.OperationException;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.handlers.AuthenticatedHttpHandler;
import com.continuuity.http.InternalHttpResponse;
import com.continuuity.metadata.MetaDataEntry;
import com.continuuity.metadata.MetaDataTable;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * Base metrics handler that can validate metrics path for existence of elements like streams, datasets, and programs.
 */
public abstract class BaseMetricsHandler extends AuthenticatedHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetricsHandler.class);
  private final MetaDataTable metaDataTable;
  //TODO: Note: MetadataTable is introduced in the dependency so that there is no call to app-fabric to find out
  // if a flow, stream, data exists. Ideal abstraction here is Store interface but that cannot be done due to
  // the way apiCompile elastic currently works in gradle
  protected BaseMetricsHandler(GatewayAuthenticator authenticator, MetaDataTable metaDataTable) {
    super(authenticator);
    this.metaDataTable = metaDataTable;
  }

  protected MetricsRequest parseAndValidate(HttpRequest request, URI requestURI)
    throws MetricsPathException, ServerException {
    ImmutablePair<MetricsRequest, MetricsRequestContext> pair = MetricsRequestParser.parseRequestAndContext(requestURI);
    validatePathElements(request, pair.getSecond());
    return pair.getFirst();
  }

  /**
   * Checks whether the elements (datasets, streams, and programs) in the context exist, throwing
   * a {@link MetricsPathException} with relevant error message if there is an element that does not exist.
   *
   * @param metricsRequestContext context containing elements whose existence we need to check.
   * @throws ServerException
   * @throws MetricsPathException
   */
  protected void validatePathElements(HttpRequest request, MetricsRequestContext metricsRequestContext)
    throws ServerException, MetricsPathException {
    String apiKey = request.getHeader(Constants.Gateway.CONTINUUITY_API_KEY);
    String accountId = getAuthenticatedAccountId(request);
    // check for existance of elements in the path
    String dataName = metricsRequestContext.getTag();
    if (dataName != null) {
      if (metricsRequestContext.getTagType() == MetricsRequestContext.TagType.STREAM) {
        if (!streamExists(dataName, apiKey)) {
          throw new MetricsPathException("stream " + dataName + " not found");
        }
      } else if (metricsRequestContext.getTagType() == MetricsRequestContext.TagType.DATASET) {
        if (!datasetExists(dataName, apiKey)) {
          throw new MetricsPathException("dataset " + dataName + " not found");
        }
      }
    }
    String appId = metricsRequestContext.getAppId();
    if (appId != null) {
      MetricsRequestParser.ProgramType programType = metricsRequestContext.getProgramType();
      String programId = metricsRequestContext.getProgramId();
      String componentId = metricsRequestContext.getComponentId();

      // if there was a flowlet in the context
      if (componentId != null && programType == MetricsRequestParser.ProgramType.FLOWS) {
        if (!flowletExists(accountId, appId, programId, componentId, apiKey)) {
          String msg = String.format("application %s with flow %s and flowlet %s not found.",
                                     appId, programId, componentId);
          throw new MetricsPathException(msg);
        }
      } else if (!programExists(programType, appId, programId, apiKey)) {
        String programMsg = (programId == null || programType == null) ? "" :
          " with " + programType.name().toLowerCase() + " " + programId;
        String msg = "application " + appId + programMsg + " not found.";
        throw new MetricsPathException(msg);
      }
    }
  }

  /**
   * Check if the given stream exists.
   *
   * @param streamName name of the stream to check for.
   * @param apiKey api key required for authentication.
   * @return true if the stream exists, false if it does not.
   * @throws ServerException if there was some problem looking up the stream.
   */
  private boolean streamExists(String streamName, String apiKey) throws ServerException {
    HttpRequest request = createRequest(HttpMethod.GET, apiKey, "/streams/" + streamName);
    InternalHttpResponse response = sendInternalRequest(request);
    // OK means it exists, NOT_FOUND means it doesn't, and anything else means there was some problem.
    if (response.getStatusCode() == HttpResponseStatus.OK.getCode()) {
      return true;
    } else if (response.getStatusCode() == HttpResponseStatus.NOT_FOUND.getCode()) {
      return false;
    } else {
      String msg = String.format("got a %d while checking if stream %s exists", response.getStatusCode(), streamName);
      throw new ServerException(msg);
    }
  }

  /**
   * Check if the given dataset exists.
   *
   * @param datasetName name of the dataset to check for.
   * @param apiKey api key required for authentication.
   * @return true if the dataset exists, false if it does not.
   * @throws ServerException if there was some problem looking up the dataset.
   */
  private boolean datasetExists(String datasetName, String apiKey) throws ServerException {
    HttpRequest request = createRequest(HttpMethod.GET, apiKey, "/datasets/" + datasetName);
    InternalHttpResponse response = sendInternalRequest(request);
    // OK means it exists, NOT_FOUND means it doesn't, and anything else means there was some problem.
    if (response.getStatusCode() == HttpResponseStatus.OK.getCode()) {
      return true;
    } else if (response.getStatusCode() == HttpResponseStatus.NOT_FOUND.getCode()) {
      return false;
    } else {
      String msg = String.format("got a %d while checking if dataset %s exists", response.getStatusCode(), datasetName);
      throw new ServerException(msg);
    }
  }

  /**
   * Check if the app, flow, and flowlet exists.
   *
   * @param accountId accountId of the user.
   * @param appId name of the application.
   * @param flowId name of the flow.
   * @param flowletId name of the flowlet.
   * @param apiKey api key required for authentication.
   * @return true if the app, flow, and flowlet exist, false if not.
   * @throws ServerException if there was some problem looking up the flowlet.
   */
  private boolean flowletExists(String accountId, String appId, String flowId, String flowletId, String apiKey)
    throws ServerException {
    boolean exists = false;
    OperationContext context = new OperationContext(accountId);
    try {
      MetaDataEntry entry = metaDataTable.get(context, accountId, null, "app",
                                              appId);
      String appSpecfication = entry.getTextField("spec");
      //TODO: The code below is not pretty. but will go away when we have mapped the dependencies better.
      JsonObject flowSpec = new Gson().fromJson(appSpecfication, JsonObject.class);
      JsonObject flows = flowSpec.getAsJsonObject("flows");
      if (flows != null ){
        JsonObject flowlets = flows.getAsJsonObject(flowId);
        if (flowlets.get(flowletId) != null){
          exists = true;
        }
      }
      return exists;
    } catch (OperationException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Check if the given type of program exists.
   *
   * @param type type of program, expected to be a flow, mapreduce or procedure.  If null, existence of just the
   *             application will be checked.
   * @param appId name of the application.
   * @param programId name of the program.  If null, existence of just the application will be checked.
   * @param apiKey api key required for authentication.
   * @return true if the program exists, false if not.
   * @throws ServerException
   */
  private boolean programExists(MetricsRequestParser.ProgramType type, String appId, String programId, String apiKey)
    throws ServerException {
    Preconditions.checkNotNull(appId, "must specify an app name to check existence for");

    String path;
    if (type == null || programId == null) {
      path = String.format("/apps/%s", appId);
    } else {
      path = String.format("/apps/%s/%s/%s", appId, type.name().toLowerCase(), programId);
    }
    HttpRequest request = createRequest(HttpMethod.GET, apiKey, path);
    InternalHttpResponse response = sendInternalRequest(request);
    // OK means it exists, NOT_FOUND means it doesn't, and anything else means there was some problem.
    if (response.getStatusCode() == HttpResponseStatus.OK.getCode()) {
      return true;
    } else if (response.getStatusCode() == HttpResponseStatus.NOT_FOUND.getCode()) {
      return false;
    } else {
      String programMsg = (programId == null || type == null) ? "" :
        " with " + type.name().toLowerCase() + " " + programId;
      String msg = "application " + appId + programMsg + " not found.";
      throw new ServerException(msg);
    }
  }

  private HttpRequest createRequest(HttpMethod method, String apiKey, String path) {
    String uri = Constants.Gateway.GATEWAY_VERSION + path;
    HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, uri);
    if (apiKey != null) {
      request.setHeader(Constants.Gateway.CONTINUUITY_API_KEY, apiKey);
    }
    return request;
  }

}
