/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.metrics.data.AggregatesScanner;
import com.continuuity.metrics.data.AggregatesTable;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Class for handling requests for aggregate application metrics of the
 * {@link com.continuuity.common.metrics.MetricsScope#USER} scope.
 */
@Path("/appmetrics")
public final class AppMetricsHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(AppMetricsHandler.class);
  private static final String CONTENT_TYPE_JSON = "application/json";

  private final AggregatesTable aggregatesTable;

  private enum MetricType {
    FLOWLETS("flowlets"),
    PROCEDURES("procedures");
    private String name;

    private MetricType(String name) {
      this.name = name;
    }

    private String getName() {
      return this.name;
    }

    private String getContextProgramType() {
      switch (this) {
        case FLOWLETS:
          return "f";
        case PROCEDURES:
          return "p";
        default:
          return null;
      }
    }
  }

  @Inject
  public AppMetricsHandler(final MetricsTableFactory metricsTableFactory) {
    this.aggregatesTable = metricsTableFactory.createAggregates(MetricsScope.USER.name());
  }

  /*
   * input json of the form:
   * {
   *   "flowlets": [
   *     {
   *       "flow":flowid,
   *       "flowlet":flowletid,
   *       "metricPrefix":metric
   *     }
   *     ...
   *   ],
   *   "procedures": [
   *     {
   *       "procedure":procedureid,
   *       "metricPrefix":metric
   *     }
   *     ...
   *   ]
   * }
   *
   * output json of the form:
   * {
   *   "flowlets": [
   *     {
   *       "flow":flowid,
   *       "flowlet":flowletid,
   *       "metricPrefix":metric,
   *       "data":count
   *     }
   *     ...
   *   ],
   *   "procedures": [
   *     {
   *       "procedure":procedureid,
   *       "metricPrefix":metric,
   *       "data":count
   *     }
   *     ...
   *   ]
   * }
   */
  @POST
  @Path("/{app-id}")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId) throws IOException {
    if (!CONTENT_TYPE_JSON.equals(request.getHeader(HttpHeaders.Names.CONTENT_TYPE))) {
      responder.sendError(HttpResponseStatus.UNSUPPORTED_MEDIA_TYPE, "Only " + CONTENT_TYPE_JSON + " is supported.");
      return;
    }

    JsonObject output = new JsonObject();
    InputStreamReader reader = new InputStreamReader(
      new ChannelBufferInputStream(request.getContent()), Charsets.UTF_8);
    try {
      JsonObject requestJson = new Gson().fromJson(reader, JsonObject.class);

      for (MetricType type : MetricType.values()) {
        JsonArray outputArray = new JsonArray();
        JsonArray metricRequests = requestJson.get(type.getName()).getAsJsonArray();
        for (JsonElement metricJson : metricRequests) {
          JsonObject metricReq = metricJson.getAsJsonObject();
          String metricPrefix = metricReq.get("metricPrefix").getAsString();
          String contextPrefix = computeContextPrefix(appId, type, metricReq);
          long data = getAggregate(contextPrefix, metricPrefix);
          metricReq.addProperty("data", data);
          outputArray.add(metricReq);
        }
        output.add(type.getName(), outputArray);
      }
    } finally {
      reader.close();
    }

    responder.sendJson(HttpResponseStatus.OK, output);
  }

  private String computeContextPrefix(String appId, MetricType type, JsonObject req) {
    StringBuilder sb = new StringBuilder();
    sb.append(appId);
    sb.append(".");
    sb.append(type.getContextProgramType());
    sb.append(".");
    if (type == MetricType.PROCEDURES) {
      sb.append(req.get("procedure").getAsString());
    } else if (type == MetricType.FLOWLETS) {
      sb.append(req.get("flow").getAsString());
      sb.append(".");
      sb.append(req.get("flowlet").getAsString());
    }
    return sb.toString();
  }

  private long getAggregate(String contextPrefix, String metricPrefix) {
    AggregatesScanner scanner = aggregatesTable.scan(contextPrefix, metricPrefix);
    long value = 0;
    while (scanner.hasNext()) {
      value += scanner.next().getValue();
    }
    return value;
  }
}
