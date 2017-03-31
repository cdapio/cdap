/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.gateway.handlers.preview;

import co.cask.cdap.app.preview.PreviewManager;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.logging.gateway.handlers.AbstractLogHandler;
import co.cask.cdap.logging.read.LogReader;
import co.cask.cdap.metrics.query.MetricsQueryHelper;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.codec.BasicThrowableCodec;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * {@link co.cask.http.HttpHandler} to manage preview lifecycle for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class PreviewHttpHandler extends AbstractLogHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewHttpHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec()).create();
  private static final Type STRING_LIST_MAP_TYPE = new TypeToken<Map<String, List<String>>>() { }.getType();
  private static final String NAMESPACE_STRING = "namespace";
  private static final String APP_STRING = "app";

  private final PreviewManager previewManager;

  @Inject
  PreviewHttpHandler(PreviewManager previewManager,
                     LogReader logReader, CConfiguration cConfig) {
    super(logReader, cConfig);
    this.previewManager = previewManager;
  }

  @POST
  @Path("/previews")
  public void start(HttpRequest request, HttpResponder responder,
                    @PathParam("namespace-id") String namespaceId) throws Exception {
    NamespaceId namespace = new NamespaceId(namespaceId);
    AppRequest appRequest;
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8)) {
      appRequest = GSON.fromJson(reader, AppRequest.class);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    }
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(previewManager.start(namespace, appRequest)));
  }

  @POST
  @Path("/previews/{preview-id}/stop")
  public void stop(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("preview-id") String previewId) throws Exception {
    NamespaceId namespace = new NamespaceId(namespaceId);
    ApplicationId application = namespace.app(previewId);
    previewManager.getRunner(application).stopPreview();
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/previews/{preview-id}/status")
  public void getStatus(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                        @PathParam("preview-id") String previewId)  throws Exception {
    NamespaceId namespace = new NamespaceId(namespaceId);
    ApplicationId application = namespace.app(previewId);
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(previewManager.getRunner(application).getStatus()));
  }

  @GET
  @Path("/previews/{preview-id}/tracers")
  public void getTracers(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                         @PathParam("preview-id") String previewId) throws Exception {
    // TODO Implement API in PreviewStore to get all the tracers.
  }

  @GET
  @Path("/previews/{preview-id}/tracers/{tracer-id}")
  public void getData(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("preview-id") String previewId,
                      @PathParam("tracer-id") String tracerId) throws Exception {
    NamespaceId namespace = new NamespaceId(namespaceId);
    ApplicationId application = namespace.app(previewId);
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(previewManager.getRunner(application).getData(tracerId)));
  }

  @POST
  @Path("/previews/{preview-id}/tracers")
  public void getTracersData(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("preview-id") String previewId) throws Exception {
    NamespaceId namespace = new NamespaceId(namespaceId);
    ApplicationId application = namespace.app(previewId);
    Map<String, List<String>> previewRequest;
    try (Reader reader = new InputStreamReader(new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8)) {
      previewRequest = GSON.fromJson(reader, STRING_LIST_MAP_TYPE);
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    }

    if (previewRequest == null) {
      throw new BadRequestException("The body is not provided.");
    }
    List<String> tracerNames = previewRequest.get("tracers");
    if (tracerNames == null || tracerNames.isEmpty()) {
      throw new BadRequestException("Tracer names cannot be empty.");
    }

    Map<String, Map<String, List<JsonElement>>> result = new HashMap<>();
    for (String tracerName : tracerNames) {
      result.put(tracerName, previewManager.getRunner(application).getData(tracerName));
    }
    responder.sendString(HttpResponseStatus.OK, GSON.toJson(result));
  }

  @GET
  @Path("/previews/{preview-id}/logs")
  public void getPreviewLogs(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId, @PathParam("preview-id") String previewId,
                             @QueryParam("start") @DefaultValue("-1") long fromTimeSecsParam,
                             @QueryParam("stop") @DefaultValue("-1") long toTimeSecsParam,
                             @QueryParam("escape") @DefaultValue("true") boolean escape,
                             @QueryParam("filter") @DefaultValue("") String filterStr,
                             @QueryParam("format") @DefaultValue("text") String format,
                             @QueryParam("suppress") List<String> suppress) throws Exception {
    ProgramRunId runId = getProgramRunId(namespaceId, previewId);
    RunRecordMeta runRecord = getRunRecord(namespaceId, previewId);
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContextWithRunId(namespaceId, previewId, runId.getProgram(), runId.getType(),
                                                      runId.getRun(), runRecord.getSystemArgs());

    doGetLogs(responder, loggingContext, fromTimeSecsParam, toTimeSecsParam, escape, filterStr, runRecord, format,
              suppress);
  }

  @GET
  @Path("/previews/{preview-id}/logs/prev")
  public void getPreviewLogsPrev(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("preview-id") String previewId,
                                 @QueryParam("max") @DefaultValue("50") int maxEvents,
                                 @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                                 @QueryParam("escape") @DefaultValue("true") boolean escape,
                                 @QueryParam("filter") @DefaultValue("") String filterStr,
                                 @QueryParam("format") @DefaultValue("text") String format,
                                 @QueryParam("suppress") List<String> suppress) throws Exception {
    ProgramRunId runId = getProgramRunId(namespaceId, previewId);
    RunRecordMeta runRecord = getRunRecord(namespaceId, previewId);
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContextWithRunId(namespaceId, previewId, runId.getProgram(), runId.getType(),
                                                      runId.getRun(), runRecord.getSystemArgs());
    doPrev(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, runRecord, format, suppress);
  }

  @GET
  @Path("/previews/{preview-id}/logs/next")
  public void getPreviewLogsNext(HttpRequest request, HttpResponder responder,
                                 @PathParam("namespace-id") String namespaceId,
                                 @PathParam("preview-id") String previewId,
                                 @QueryParam("max") @DefaultValue("50") int maxEvents,
                                 @QueryParam("fromOffset") @DefaultValue("") String fromOffsetStr,
                                 @QueryParam("escape") @DefaultValue("true") boolean escape,
                                 @QueryParam("filter") @DefaultValue("") String filterStr,
                                 @QueryParam("format") @DefaultValue("text") String format,
                                 @QueryParam("suppress") List<String> suppress) throws Exception {
    ProgramRunId runId = getProgramRunId(namespaceId, previewId);
    RunRecordMeta runRecord = getRunRecord(namespaceId, previewId);
    LoggingContext loggingContext =
      LoggingContextHelper.getLoggingContextWithRunId(namespaceId, previewId, runId.getProgram(), runId.getType(),
                                                      runId.getRun(), runRecord.getSystemArgs());
    doNext(responder, loggingContext, maxEvents, fromOffsetStr, escape, filterStr, runRecord, format, suppress);
  }

  @POST
  @Path("previews/{preview-id}/metrics/search")
  public void search(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("preview-id") String previewId,
                     @QueryParam("target") String target,
                     @QueryParam("tag") List<String> tags) throws Exception {
    MetricsQueryHelper helper = getMetricsQueryHelper(namespaceId, previewId);
    if (target == null) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Required target param is missing");
      return;
    }
    try {
      tags = overrideTags(tags, namespaceId, previewId);
      switch (target) {
        case "tag":
          responder.sendJson(HttpResponseStatus.OK, helper.searchTags(tags));
          break;
        case "metric":
          responder.sendJson(HttpResponseStatus.OK, helper.searchMetric(tags));
          break;
        default:
          responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Unknown target param value: " + target);
          break;
      }
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    }
  }

  @POST
  @Path("previews/{preview-id}/metrics/query")
  public void query(HttpRequest request, HttpResponder responder,
                    @PathParam("namespace-id") String namespaceId,
                    @PathParam("preview-id") String previewId,
                    @QueryParam("metric") List<String> metrics,
                    @QueryParam("groupBy") List<String> groupBy,
                    @QueryParam("tag") List<String> tags) throws Exception {
    MetricsQueryHelper helper = getMetricsQueryHelper(namespaceId, previewId);
    try {
      if (new QueryStringDecoder(request.getUri()).getParameters().isEmpty()) {
        if (HttpHeaders.getContentLength(request) > 0) {
          Map<String, MetricsQueryHelper.QueryRequestFormat> queries =
            GSON.fromJson(request.getContent().toString(Charsets.UTF_8),
                          new TypeToken<Map<String, MetricsQueryHelper.QueryRequestFormat>>() { }.getType());
          overrideQueries(queries, namespaceId, previewId);
          responder.sendJson(HttpResponseStatus.OK, helper.executeBatchQueries(queries));
          return;
        }
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Batch request with empty content");
      }
      tags = overrideTags(tags, namespaceId, previewId);
      responder.sendJson(HttpResponseStatus.OK,
                         helper.executeTagQuery(tags, metrics, groupBy,
                                                new QueryStringDecoder(request.getUri()).getParameters()));
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    }
  }

  private ProgramRunId getProgramRunId(String namespaceId, String previewId) throws Exception {
     return previewManager.getRunner(new ApplicationId(namespaceId, previewId)).getProgramRunId();
  }

  private RunRecordMeta getRunRecord(String namespaceId, String previewId) throws Exception {
    return previewManager.getRunner(new ApplicationId(namespaceId, previewId)).getRunRecord();
  }

  private MetricsQueryHelper getMetricsQueryHelper(String namespaceId, String previewId) throws Exception {
    return previewManager.getRunner(new ApplicationId(namespaceId, previewId)).getMetricsQueryHelper();
  }

  private List<String> overrideTags(List<String> tags, String namespaceId, String previewId) {
    List<String> newTags = Lists.newArrayList();
    String namespaceTag = NAMESPACE_STRING + ":" + namespaceId;
    String previewTag = APP_STRING + ":" + previewId;
    for (String tag : tags) {
      if (tag.startsWith(NAMESPACE_STRING)) {
        newTags.add(namespaceTag);
      } else if (tag.startsWith(APP_STRING)) {
        newTags.add(previewTag);
      } else {
        newTags.add(tag);
      }
    }
    return newTags;
  }

  private void overrideQueries(Map<String, MetricsQueryHelper.QueryRequestFormat> query, String namespaceId,
                               String previewId) {
    for (MetricsQueryHelper.QueryRequestFormat format : query.values()) {
      Map<String, String> tags = format.getTags();
      if (!tags.containsKey(NAMESPACE_STRING) || !tags.get(NAMESPACE_STRING).equals(namespaceId)) {
        tags.put(NAMESPACE_STRING, namespaceId);
      }
      if (!tags.containsKey(APP_STRING) || !tags.get(APP_STRING).equals(previewId)) {
        tags.put(APP_STRING, previewId);
      }
    }
  }
}
