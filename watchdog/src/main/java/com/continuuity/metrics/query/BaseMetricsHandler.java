/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.service.ServerException;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.OperationException;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.AuthenticatedHttpHandler;
import com.continuuity.metadata.MetaDataEntry;
import com.continuuity.metadata.MetaDataTable;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
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
  //TODO: Note: MetadataTable is introduced in the dependency as a temproary solution. REACTOR-12 jira has details
  protected BaseMetricsHandler(Authenticator authenticator, MetaDataTable metaDataTable) {
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
        if (!streamExists(accountId, dataName)) {
          throw new MetricsPathException("stream " + dataName + " not found");
        }
      } else if (metricsRequestContext.getTagType() == MetricsRequestContext.TagType.DATASET) {
        if (!datasetExists(accountId, dataName)) {
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
      } else if (!programExists(programType, accountId, appId, programId, apiKey)) {
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
   * @param accountId accountId of the user.
   * @param streamName name of the stream to check for.
   * @return true if the stream exists, false if it does not.
   * @throws ServerException if there was some problem looking up the stream.
   */
  private boolean streamExists(String accountId, String streamName) throws ServerException {
    OperationContext context = new OperationContext(accountId);
    try {
      MetaDataEntry entry = metaDataTable.get(context, accountId, null, "str", streamName);
      return entry !=  null ? true : false;
    } catch (OperationException e) {
      throw new ServerException(e.getMessage(), e.getCause());
    }
  }

  /**
   * Check if the given dataset exists.
   *
   * @param accountId accountId of the user.
   * @param datasetName name of the dataset to check for.
   * @return true if the dataset exists, false if it does not.
   * @throws ServerException if there was some problem looking up the dataset.
   */
  private boolean datasetExists(String accountId, String datasetName) throws ServerException {
    OperationContext context = new OperationContext(accountId);
    try {
      MetaDataEntry entry = metaDataTable.get(context, accountId, null, "ds", datasetName);
      return entry !=  null ? true : false;
    } catch (OperationException e) {
      throw new ServerException(e.getMessage(), e.getCause());
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
      //TODO: The code below is not pretty. but will go away when REACTOR-12 is fixed.
      JsonObject appSpec = new Gson().fromJson(appSpecfication, JsonObject.class);
      JsonObject allFlows = appSpec.getAsJsonObject("flows");
      if (allFlows != null) {
        JsonObject flow = allFlows.getAsJsonObject(flowId);
        if (flow != null) {
          JsonObject flowlets = flow.getAsJsonObject("flowlets");
          if (flowlets != null && flowlets.get(flowletId) != null) {
            exists = true;
          }
        }
      }
      return exists;
    } catch (OperationException e) {
      throw new ServerException(e.getMessage(), e.getCause());
    }
  }

  /**
   * Check if the given type of program exists.
   *
   * @param type type of program, expected to be a flow, mapreduce or procedure.  If null, existence of just the
   *             application will be checked.
   * @param accountId accountId of the user.
   * @param appId name of the application.
   * @param programId name of the program.  If null, existence of just the application will be checked.
   * @param apiKey api key required for authentication.
   * @return true if the program exists, false if not.
   * @throws ServerException
   */
  private boolean programExists(MetricsRequestParser.ProgramType type, String accountId,
                                String appId, String programId, String apiKey)
    throws ServerException {
    Preconditions.checkNotNull(appId, "must specify an app name to check existence for");
    OperationContext context = new OperationContext(accountId);
    boolean exists = false;
    try {
      MetaDataEntry entry = metaDataTable.get(context, accountId, null, "app", appId);
      if (entry != null) {
        if (type == null || programId == null) {
          // type and programId is null. So the look up is for app and it exists because the metadata entry is not null.
          exists = true;
        } else {
          String spec = entry.getTextField("spec");
          JsonObject appSpecfication = new Gson().fromJson(spec, JsonObject.class);
          JsonObject programSpec = null;
          switch (type) {
            case MAPREDUCE:
              programSpec = appSpecfication.getAsJsonObject("mapReduces");
              break;
            case PROCEDURES:
              programSpec = appSpecfication.getAsJsonObject("procedures");
              break;
            case FLOWS:
              programSpec = appSpecfication.getAsJsonObject("flows");
              break;
          }
          if (programSpec != null) {
            JsonObject program = programSpec.getAsJsonObject(programId);
            if (program != null) {
              exists = true;
            }
          }
        }
      }
    } catch (OperationException e) {
      throw new ServerException(e.getMessage(), e.getCause());
    }
    return exists;
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
