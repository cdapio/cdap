/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.metrics.data.AggregatesScanResult;
import com.continuuity.metrics.data.AggregatesScanner;
import com.continuuity.metrics.data.AggregatesTable;
import com.continuuity.metrics.data.MetricsTableFactory;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Class for handling requests for aggregate application metrics of the
 * {@link com.continuuity.common.metrics.MetricsScope#USER} scope.
 */
@Path("/metrics")
public final class MetricsDiscoveryHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsDiscoveryHandler.class);

  private final Map<MetricsScope, AggregatesTable> aggregatesTables;

  private enum PathProgramType {
    PROCEDURES("p"),
    MAPREDUCES("b"),
    FLOWS("f");
    String id;

    private PathProgramType(String id) {
      this.id = id;
    }

    private String getId() {
      return id;
    }
  }

  private enum ContextNodeType {
    APP("app"),
    FLOW("flow"),
    FLOWLET("flowlet"),
    PROCEDURE("procedure"),
    MAPREDUCE("mapreduce"),
    MAPREDUCE_TASK("mapreduceTask"),
    STREAM("stream"),
    DATASET("dataset");
    String name;

    private ContextNodeType(String name) {
      this.name = name;
    }

    private String getName() {
      return name;
    }
  }

  private enum ProgramType {
    P,
    B,
    F,
    STREAM,
    DATASET;
  }

  @Inject
  public MetricsDiscoveryHandler(final MetricsTableFactory metricsTableFactory) {
    this.aggregatesTables = Maps.newHashMap();
    for (MetricsScope scope : MetricsScope.values()) {
      this.aggregatesTables.put(scope, metricsTableFactory.createAggregates(scope.name()));
    }
  }

  @GET
  @Path("/available")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder) throws IOException {
    responder.sendJson(HttpResponseStatus.OK, getMetrics(null, ""));
  }

  @GET
  @Path("/available/app/{app-id}")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId) throws IOException {
    responder.sendJson(HttpResponseStatus.OK, getMetrics(appId, ""));
  }

  // @TODO(albert) add query params for metric prefix
  @GET
  @Path("/available/app/{app-id}/{program-type}")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId,
                                      @PathParam("program-type") String programType) throws IOException {
    String programTypeId = PathProgramType.valueOf(programType.toUpperCase()).getId();
    String context = Joiner.on(".").join(appId, programTypeId);
    responder.sendJson(HttpResponseStatus.OK, getMetrics(context, ""));
  }

  @GET
  @Path("/available/app/{app-id}/{program-type}/{program-id}")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId,
                                      @PathParam("program-type") String programType,
                                      @PathParam("program-id") String programId) throws IOException {
    String programTypeId = PathProgramType.valueOf(programType.toUpperCase()).getId();
    String context = Joiner.on(".").join(appId, programTypeId, programId);
    responder.sendJson(HttpResponseStatus.OK, getMetrics(context, ""));
  }

  @GET
  @Path("/available/app/{app-id}/{program-type}/{program-id}/{component-id}")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId,
                                      @PathParam("program-type") String programType,
                                      @PathParam("program-id") String programId,
                                      @PathParam("component-id") String componentId) throws IOException {
    String programTypeId = PathProgramType.valueOf(programType.toUpperCase()).getId();
    String context = Joiner.on(".").join(appId, programTypeId, programId, componentId);
    responder.sendJson(HttpResponseStatus.OK, getMetrics(context, ""));
  }

  private JsonArray getMetrics(String contextPrefix, String metricPrefix) {

    // metric -> app -> appContext
    Map<String, Map<String, ContextNode>> metricContextsMap = Maps.newHashMap();
    for (Map.Entry<MetricsScope, AggregatesTable> entry : this.aggregatesTables.entrySet()) {
      AggregatesTable table = entry.getValue();
      AggregatesScanner scanner = table.scan(contextPrefix, metricPrefix);

      // scanning through all metric rows in the aggregates table
      // row has context plus metric info
      // each metric can show up in multiple contexts
      while (scanner.hasNext()) {
        AggregatesScanResult result = scanner.next();
        String metric = result.getMetric();
        if (!metricContextsMap.containsKey(metric)) {
          Map<String, ContextNode> metricContext = Maps.newHashMap();
          metricContextsMap.put(metric, metricContext);
        }
        addContext(result.getContext(), metricContextsMap.get(metric));
      }
    }

    JsonArray output = new JsonArray();
    for (Map.Entry<String, Map<String, ContextNode>> metricContexts : metricContextsMap.entrySet()) {
      JsonObject metricNode = new JsonObject();
      metricNode.addProperty("metric", metricContexts.getKey());
      JsonArray contexts = new JsonArray();
      for (ContextNode appContext : metricContexts.getValue().values()) {
        contexts.add(appContext.toJson());
      }
      metricNode.add("contexts", contexts);
      output.add(metricNode);
    }
    return output;
  }

  private void addContext(String context, Map<String, ContextNode> metricContextsMap) {
    Iterator<String> contextParts = Splitter.on('.').split(context).iterator();
    if (!contextParts.hasNext()) {
      return;
    }
    String appId = contextParts.next();
    if (!contextParts.hasNext()) {
      return;
    }

    if (!metricContextsMap.containsKey(appId)) {
      metricContextsMap.put(appId, new ContextNode(ContextNodeType.APP, appId));
    }
    ContextNode metricContexts = metricContextsMap.get(appId);

    ProgramType type = ProgramType.valueOf(contextParts.next().toUpperCase());
    switch(type) {
      case F:
        metricContexts.deepAdd(contextParts, ContextNodeType.FLOW, ContextNodeType.FLOWLET);
        break;
      case P:
        metricContexts.deepAdd(contextParts, ContextNodeType.PROCEDURE);
        break;
      case B:
        metricContexts.deepAdd(contextParts, ContextNodeType.MAPREDUCE, ContextNodeType.MAPREDUCE_TASK);
        break;
      // dataset and stream are special cases, currently the only "context" for them is "-.dataset" and "-.stream"
      case DATASET:
        metricContexts.addChild(new ContextNode(ContextNodeType.DATASET, ""));
        break;
      case STREAM:
        metricContexts.addChild(new ContextNode(ContextNodeType.STREAM, ""));
        break;
      default:
        break;
    }
  }

  private class ContextNode {
    ContextNodeType type;
    String id;
    Map<String, ContextNode> children;

    public ContextNode(ContextNodeType type, String id) {
      this.type = type;
      this.id = id;
      this.children = Maps.newHashMap();
    }

    public ContextNode getOrAddChild(ContextNodeType type, String id) {
      String key = getChildKey(type, id);
      if (!children.containsKey(key)) {
        children.put(key, new ContextNode(type, id));
      }
      return children.get(key);
    }

    public void deepAdd(Iterator<String> ids, ContextNodeType... types) {
      ContextNode node = this;
      for (int i = 0; i < types.length; i++) {
        if (!ids.hasNext()) {
          break;
        }
        node = node.getOrAddChild(types[i], ids.next());
      }
    }

    public void addChild(ContextNode child) {
      children.put(getChildKey(child.getType(), child.getId()), child);
    }

    public ContextNodeType getType() {
      return type;
    }

    public String getId() {
      return id;
    }

    private String getChildKey(ContextNodeType type, String id) {
      return type.name() + ":" + id;
    }

    public JsonObject toJson() {
      JsonObject output = new JsonObject();
      output.addProperty("type", type.getName());
      output.addProperty("id", id);
      if (children.size() > 0) {
        JsonArray childrenJson = new JsonArray();
        for (ContextNode child : children.values()) {
          childrenJson.add(child.toJson());
        }
        output.add("children", childrenJson);
      }
      return output;
    }
  }

}
