/*
 * Copyright © 2015-2019 Cask Data, Inc.
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

package io.cdap.cdap.metrics.query;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import io.cdap.cdap.api.security.AccessException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.proto.MetricTagValue;
import io.cdap.cdap.proto.id.InstanceId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.spi.authorization.ContextAccessEnforcer;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpResponder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.QueryStringDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Search metrics handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/metrics")
public class MetricsHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsHandler.class);
  private static final Gson GSON = new Gson();

  private final MetricsQueryHelper metricsQueryHelper;
  private final ContextAccessEnforcer contextAccessEnforcer;

  @Inject
  public MetricsHandler(MetricsQueryHelper metricsQueryHelper,
      ContextAccessEnforcer contextAccessEnforcer) {
    this.metricsQueryHelper = metricsQueryHelper;
    this.contextAccessEnforcer = contextAccessEnforcer;
  }

  @POST
  @Path("/search")
  public void search(HttpRequest request, HttpResponder responder,
      @QueryParam("target") String target,
      @QueryParam("tag") List<String> tags) throws Exception {
    if (target == null) {
      responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Required target param is missing");
      return;
    }
    try {
      enforceOnTags(tags);
      switch (target) {
        case "tag":
          responder.sendJson(HttpResponseStatus.OK,
              GSON.toJson(metricsQueryHelper.searchTags(tags)));
          break;
        case "metric":
          responder.sendJson(HttpResponseStatus.OK,
              GSON.toJson(metricsQueryHelper.searchMetric(tags)));
          break;
        default:
          responder.sendJson(HttpResponseStatus.BAD_REQUEST,
              "Unknown target param value: " + target);
          break;
      }
    } catch (AccessException | ServiceUnavailableException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
          "Internal error while querying for metrics");
    }
  }

  /**
   * REST endpoint for metrics query
   *
   * @param metrics the list metrics name
   * @param groupBy the list of tag names to group the results
   * @param tags the tags of the metric names
   */
  @POST
  @Path("/query")
  public void query(FullHttpRequest request, HttpResponder responder,
      @QueryParam("metric") List<String> metrics,
      @QueryParam("groupBy") List<String> groupBy,
      @QueryParam("tag") List<String> tags) {
    try {
      Map<String, List<String>> queryParams = new QueryStringDecoder(request.uri()).parameters();
      if (!queryParams.isEmpty()) {
        enforceOnTags(tags);
        responder.sendJson(HttpResponseStatus.OK,
            GSON.toJson(metricsQueryHelper.executeTagQuery(tags, metrics, groupBy, queryParams)));
        return;
      }
      Map<String, MetricsQueryHelper.QueryRequestFormat> queries =
          HttpUtil.getContentLength(request) <= 0 ? null
              : GSON.fromJson(request.content().toString(StandardCharsets.UTF_8),
                  new TypeToken<Map<String, MetricsQueryHelper.QueryRequestFormat>>() {
                  }.getType());
      if (queries == null) {
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Batch request with empty content");
        return;
      }
      if (queries.isEmpty()) {
        enforceOnNamespaces(Collections.emptySet());
      }
      // Each query is executed independently, so each one must be authorized on its own.
      for (Map.Entry<String, MetricsQueryHelper.QueryRequestFormat> entry : queries.entrySet()) {
        if (entry.getValue() == null) {
          responder.sendJson(HttpResponseStatus.BAD_REQUEST,
              String.format("Batch query '%s' cannot be null", entry.getKey()));
          return;
        }
        enforceOnTagMap(entry.getValue().getTags());
      }
      responder.sendJson(HttpResponseStatus.OK,
          GSON.toJson(metricsQueryHelper.executeBatchQueries(queries)));
    } catch (AccessException | ServiceUnavailableException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
          "Internal error while querying for metrics");
    }
  }

  @GET
  @Path("/processor/status")
  public void processorStatus(HttpRequest request, HttpResponder responder) throws Exception {
    contextAccessEnforcer.enforce(InstanceId.SELF, StandardPermission.UPDATE);
    responder.sendJson(HttpResponseStatus.OK,
        GSON.toJson(metricsQueryHelper.getMetricStore().getMetricsProcessorStats()));
  }

  private void enforceOnTags(@Nullable List<String> tags) {
    if (tags == null) {
      enforceOnNamespaces(Collections.emptySet());
      return;
    }
    Set<String> namespaces = MetricsQueryHelper.parseTagValues(tags).stream()
        .filter(tag -> isNamespaceTag(tag.getName()))
        .map(MetricTagValue::getValue)
        .collect(Collectors.toSet());
    enforceOnNamespaces(namespaces);
  }

  private void enforceOnTagMap(@Nullable Map<String, String> tags) {
    if (tags == null) {
      enforceOnNamespaces(Collections.emptySet());
      return;
    }
    Set<String> namespaces = tags.entrySet().stream()
        .filter(tag -> isNamespaceTag(tag.getKey()))
        .map(Map.Entry::getValue)
        .collect(Collectors.toSet());
    enforceOnNamespaces(namespaces);
  }

  private static boolean isNamespaceTag(String tagName) {
    return Constants.Metrics.Tag.NAMESPACE.equals(MetricsQueryHelper.humanToTagName(tagName));
  }

  // Unscoped, wildcard and system namespace queries need instance UPDATE; others need GET on each.
  private void enforceOnNamespaces(Set<String> namespaces) {
    if (namespaces.isEmpty() || namespaces.contains(null)
        || namespaces.contains(MetricsQueryHelper.ANY_TAG_VALUE)) {
      contextAccessEnforcer.enforce(InstanceId.SELF, StandardPermission.UPDATE);
      return;
    }
    for (String namespace : namespaces) {
      if (NamespaceId.SYSTEM.getNamespace().equalsIgnoreCase(namespace)) {
        contextAccessEnforcer.enforce(InstanceId.SELF, StandardPermission.UPDATE);
      } else {
        contextAccessEnforcer.enforce(new NamespaceId(namespace), StandardPermission.GET);
      }
    }
  }
}
