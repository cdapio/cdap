/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.metrics.query;

import co.cask.cdap.common.conf.Constants;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Search metrics handler.
 */
@Path(Constants.Gateway.API_VERSION_3 + "/metrics")
public class MetricsHandler extends AbstractHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MetricsHandler.class);
  private static final Gson GSON = new Gson();

  private final MetricsQueryHelper metricsQueryHelper;

  @Inject
  public MetricsHandler(MetricsQueryHelper metricsQueryHelper) {
    this.metricsQueryHelper = metricsQueryHelper;
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
      switch (target) {
        case "tag":
          responder.sendJson(HttpResponseStatus.OK, metricsQueryHelper.searchTags(tags));
          break;
        case "metric":
          responder.sendJson(HttpResponseStatus.OK, metricsQueryHelper.searchMetric(tags));
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
  @Path("/query")
  public void query(HttpRequest request, HttpResponder responder,
                    @QueryParam("metric") List<String> metrics,
                    @QueryParam("groupBy") List<String> groupBy,
                    @QueryParam("tag") List<String> tags) throws Exception {
    try {
      if (new QueryStringDecoder(request.getUri()).getParameters().isEmpty()) {
        if (HttpHeaders.getContentLength(request) > 0) {
          Map<String, MetricsQueryHelper.QueryRequestFormat> queries =
            GSON.fromJson(request.getContent().toString(Charsets.UTF_8),
                          new TypeToken<Map<String, MetricsQueryHelper.QueryRequestFormat>>() { }.getType());
          responder.sendJson(HttpResponseStatus.OK, metricsQueryHelper.executeBatchQueries(queries));
          return;
        }
        responder.sendJson(HttpResponseStatus.BAD_REQUEST, "Batch request with empty content");
      }
      responder.sendJson(HttpResponseStatus.OK,
                         metricsQueryHelper.executeTagQuery(tags, metrics, groupBy,
                                                            new QueryStringDecoder(request.getUri()).getParameters()));
    } catch (IllegalArgumentException e) {
      LOG.warn("Invalid request", e);
      responder.sendString(HttpResponseStatus.BAD_REQUEST, e.getMessage());
    } catch (Exception e) {
      LOG.error("Exception querying metrics ", e);
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, "Internal error while querying for metrics");
    }
  }
}
