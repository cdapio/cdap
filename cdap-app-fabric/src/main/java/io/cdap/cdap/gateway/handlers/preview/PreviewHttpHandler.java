/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.cdap.gateway.handlers.preview;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.app.preview.PreviewManager;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.io.CaseInsensitiveEnumTypeAdapterFactory;
import io.cdap.cdap.common.logging.LoggingContext;
import io.cdap.cdap.internal.app.store.RunRecordMeta;
import io.cdap.cdap.logging.context.LoggingContextHelper;
import io.cdap.cdap.logging.gateway.handlers.AbstractLogHandler;
import io.cdap.cdap.logging.read.LogReader;
import io.cdap.cdap.metrics.query.MetricsQueryHelper;
import io.cdap.cdap.proto.BasicThrowable;
import io.cdap.cdap.proto.artifact.AppRequest;
import io.cdap.cdap.proto.codec.BasicThrowableCodec;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.http.HttpResponder;
import io.netty.buffer.ByteBufInputStream;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

/**
 * {@link io.cdap.http.HttpHandler} to manage preview lifecycle for v3 REST APIs
 */
@Singleton
@Path(Constants.Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class PreviewHttpHandler extends AbstractLogHandler {
  private static final Logger LOG = LoggerFactory.getLogger(PreviewHttpHandler.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(BasicThrowable.class, new BasicThrowableCodec())
    .registerTypeAdapterFactory(new CaseInsensitiveEnumTypeAdapterFactory(true)).create();
  private static final Type STRING_LIST_MAP_TYPE = new TypeToken<Map<String, List<String>>>() { }.getType();

  private final PreviewManager previewManager;

  @Inject
  PreviewHttpHandler(PreviewManager previewManager, CConfiguration cConf) {
    super(cConf);
    this.previewManager = previewManager;
  }

  @POST
  @Path("/previews")
  public void start(FullHttpRequest request, HttpResponder responder,
                    @PathParam("namespace-id") String namespaceId) throws Exception {
    NamespaceId namespace = new NamespaceId(namespaceId);
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
      AppRequest<?> appRequest = GSON.fromJson(reader, AppRequest.class);
      responder.sendString(HttpResponseStatus.OK, GSON.toJson(previewManager.start(namespace, appRequest)));
    } catch (JsonSyntaxException e) {
      throw new BadRequestException("Request body is invalid json: " + e.getMessage());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e.getMessage());
    }
  }

  @POST
  @Path("/previews/{preview-id}/stop")
  public void stop(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                   @PathParam("preview-id") String previewId) throws Exception {
    NamespaceId namespace = new NamespaceId(namespaceId);
    ApplicationId application = namespace.app(previewId);
    previewManager.stopPreview(application);
    responder.sendStatus(HttpResponseStatus.OK);
  }

  @GET
  @Path("/previews/{preview-id}/status")
  public void getStatus(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                        @PathParam("preview-id") String previewId)  throws Exception {
    NamespaceId namespace = new NamespaceId(namespaceId);
    ApplicationId application = namespace.app(previewId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(previewManager.getStatus(application)));
  }

  @GET
  @Path("/previews/{preview-id}/tracers")
  public void getTracers(HttpRequest request, HttpResponder responder, @PathParam("namespace-id") String namespaceId,
                         @PathParam("preview-id") String previewId) {
    // TODO Implement API in PreviewStore to get all the tracers.
  }

  @GET
  @Path("/previews/{preview-id}/tracers/{tracer-id}")
  public void getData(HttpRequest request, HttpResponder responder,
                      @PathParam("namespace-id") String namespaceId,
                      @PathParam("preview-id") String previewId,
                      @PathParam("tracer-id") String tracerId) throws Exception {
    ApplicationId previewAppId = validateAndGetAppId(namespaceId, previewId);
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(previewManager.getData(previewAppId, tracerId)));
  }

  @POST
  @Path("/previews/{preview-id}/tracers")
  public void getTracersData(FullHttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId,
                             @PathParam("preview-id") String previewId) throws Exception {
    ApplicationId application = validateAndGetAppId(namespaceId, previewId);

    Map<String, List<String>> previewRequest;
    try (Reader reader = new InputStreamReader(new ByteBufInputStream(request.content()), StandardCharsets.UTF_8)) {
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
      result.put(tracerName, previewManager.getData(application, tracerName));
    }
    responder.sendJson(HttpResponseStatus.OK, GSON.toJson(result));
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
    sendLogs(responder, namespaceId, previewId,
             info -> doGetLogs(info.getLogReader(), responder, info.getLoggingContext(),
                               fromTimeSecsParam, toTimeSecsParam, escape, filterStr,
                               info.getRunRecord(), format, suppress));
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
    sendLogs(responder, namespaceId, previewId,
             info -> doPrev(info.getLogReader(), responder, info.getLoggingContext(), maxEvents,
                            fromOffsetStr, escape, filterStr, info.getRunRecord(), format, suppress));
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
    sendLogs(responder, namespaceId, previewId,
             info -> doNext(info.getLogReader(), responder, info.getLoggingContext(), maxEvents,
                            fromOffsetStr, escape, filterStr, info.getRunRecord(), format, suppress));
  }

  private void sendLogs(HttpResponder responder, String namespaceId, String previewId,
                        Consumer<LogReaderInfo> logsResponder) throws Exception {
    ApplicationId applicationId = new ApplicationId(namespaceId, previewId);
    ProgramRunId programRunId = previewManager.getRunId(applicationId);
    if (programRunId == null) {
      responder.sendStatus(HttpResponseStatus.OK);
      return;
    }
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRunId, null);
    LogReader logReader = previewManager.getLogReader();
    logsResponder.accept(new LogReaderInfo(logReader, loggingContext, null));
  }

  @POST
  @Path("previews/{preview-id}/metrics/search")
  public void search(HttpRequest request, HttpResponder responder,
                     @PathParam("namespace-id") String namespaceId,
                     @PathParam("preview-id") String previewId,
                     @QueryParam("target") String target,
                     @QueryParam("tag") List<String> tags) throws Exception {
    validateAndGetAppId(namespaceId, previewId);

    MetricsQueryHelper helper = getMetricsQueryHelper();
    if (target == null) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Required target param is missing");
      return;
    }
    try {
      tags = overrideTags(tags, namespaceId, previewId);
      switch (target) {
        case "tag":
          responder.sendJson(HttpResponseStatus.OK, GSON.toJson(helper.searchTags(tags)));
          break;
        case "metric":
          responder.sendJson(HttpResponseStatus.OK, GSON.toJson(helper.searchMetric(tags)));
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
  public void query(FullHttpRequest request, HttpResponder responder,
                    @PathParam("namespace-id") String namespaceId,
                    @PathParam("preview-id") String previewId,
                    @QueryParam("metric") List<String> metrics,
                    @QueryParam("groupBy") List<String> groupBy,
                    @QueryParam("tag") List<String> tags) throws Exception {
    // Check if valid application id
    validateAndGetAppId(namespaceId, previewId);

    MetricsQueryHelper helper = getMetricsQueryHelper();
    try {
      Map<String, List<String>> queryParameters = new QueryStringDecoder(request.uri()).parameters();
      if (queryParameters.isEmpty()) {
        if (request.content().isReadable()) {
          Map<String, MetricsQueryHelper.QueryRequestFormat> queries =
            GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
                          new TypeToken<Map<String, MetricsQueryHelper.QueryRequestFormat>>() { }.getType());
          overrideQueries(queries, namespaceId, previewId);
          responder.sendJson(HttpResponseStatus.OK, GSON.toJson(helper.executeBatchQueries(queries)));
          return;
        }
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Batch request with empty content");
      }
      tags = overrideTags(tags, namespaceId, previewId);
      responder.sendJson(HttpResponseStatus.OK,
                         GSON.toJson(helper.executeTagQuery(tags, metrics, groupBy, queryParameters)));
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    }
  }

  private MetricsQueryHelper getMetricsQueryHelper() {
    return previewManager.getMetricsQueryHelper();
  }

  private List<String> overrideTags(List<String> tags, String namespaceId, String previewId) {
    List<String> newTags = Lists.newArrayList();
    String namespaceTag = MetricsQueryHelper.NAMESPACE_STRING + ":" + namespaceId;
    String previewTag = MetricsQueryHelper.APP_STRING + ":" + previewId;
    for (String tag : tags) {
      if (tag.startsWith(MetricsQueryHelper.NAMESPACE_STRING)) {
        newTags.add(namespaceTag);
      } else if (tag.startsWith(MetricsQueryHelper.APP_STRING)) {
        newTags.add(previewTag);
      } else {
        newTags.add(tag);
      }
    }
    if (!newTags.contains(namespaceTag)) {
      newTags.add(namespaceTag);
    }
    if (!newTags.contains(previewTag)) {
      newTags.add(previewTag);
    }
    return newTags;
  }

  private void overrideQueries(Map<String, MetricsQueryHelper.QueryRequestFormat> query, String namespaceId,
                               String previewId) {
    for (MetricsQueryHelper.QueryRequestFormat format : query.values()) {
      Map<String, String> tags = format.getTags();
      if (!tags.containsKey(MetricsQueryHelper.NAMESPACE_STRING) ||
        !tags.get(MetricsQueryHelper.NAMESPACE_STRING).equals(namespaceId)) {
        tags.put(MetricsQueryHelper.NAMESPACE_STRING, namespaceId);
      }
      if (!tags.containsKey(MetricsQueryHelper.APP_STRING) ||
        !tags.get(MetricsQueryHelper.APP_STRING).equals(previewId)) {
        tags.put(MetricsQueryHelper.APP_STRING, previewId);
      }
    }
  }

  // Create the application ID for the supplied namespace and preview if its valid.
  // Validity is verified by checking its status in the store.
  private ApplicationId validateAndGetAppId(String namespace, String preview) throws NotFoundException {
    ApplicationId applicationId = new ApplicationId(namespace, preview);
    previewManager.getStatus(applicationId);
    return applicationId;
  }

  /**
   * A holder class for holding information needed for reading logs.
   */
  private static final class LogReaderInfo {

    private final LogReader logReader;
    private final LoggingContext loggingContext;
    private final RunRecordMeta runRecord;

    private LogReaderInfo(LogReader logReader, LoggingContext loggingContext, RunRecordMeta runRecord) {
      this.logReader = logReader;
      this.loggingContext = loggingContext;
      this.runRecord = runRecord;
    }

    LogReader getLogReader() {
      return logReader;
    }

    LoggingContext getLoggingContext() {
      return loggingContext;
    }

    RunRecordMeta getRunRecord() {
      return runRecord;
    }
  }
}
