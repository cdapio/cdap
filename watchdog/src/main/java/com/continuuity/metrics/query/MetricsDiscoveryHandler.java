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
import com.google.common.collect.Lists;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
    DATASET("dataset"),
    ROOT("root");
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
  @Path("/available/apps/{app-id}")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId) throws IOException {
    responder.sendJson(HttpResponseStatus.OK, getMetrics(appId, ""));
  }

  // @TODO(albert) add query params for metric prefix
  @GET
  @Path("/available/apps/{app-id}/{program-type}")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId,
                                      @PathParam("program-type") String programType) throws IOException {
    String programTypeId = PathProgramType.valueOf(programType.toUpperCase()).getId();
    String context = Joiner.on(".").join(appId, programTypeId);
    responder.sendJson(HttpResponseStatus.OK, getMetrics(context, ""));
  }

  @GET
  @Path("/available/apps/{app-id}/{program-type}/{program-id}")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId,
                                      @PathParam("program-type") String programType,
                                      @PathParam("program-id") String programId) throws IOException {
    String programTypeId = PathProgramType.valueOf(programType.toUpperCase()).getId();
    String context = Joiner.on(".").join(appId, programTypeId, programId);
    responder.sendJson(HttpResponseStatus.OK, getMetrics(context, ""));
  }

  @GET
  @Path("/available/apps/{app-id}/{program-type}/{program-id}/{component-id}")
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

    Map<String, ContextNode> metricContextsMap = Maps.newHashMap();
    for (Map.Entry<MetricsScope, AggregatesTable> entry : this.aggregatesTables.entrySet()) {
      AggregatesTable table = entry.getValue();
      AggregatesScanner scanner = table.scan(contextPrefix, metricPrefix);

      // scanning through all metric rows in the aggregates table
      // row has context plus metric info
      // each metric can show up in multiple contexts
      while (scanner.hasNext()) {
        AggregatesScanResult result = scanner.next();
        addContext(result.getContext(), result.getMetric(), metricContextsMap);
      }
    }

    JsonArray output = new JsonArray();
    List<String> sortedMetrics = Lists.newArrayList(metricContextsMap.keySet());
    Collections.sort(sortedMetrics);
    for (String metric : sortedMetrics) {
      JsonObject metricNode = new JsonObject();
      metricNode.addProperty("metric", metric);
      ContextNode metricContexts = metricContextsMap.get(metric);
      // the root node has junk for its type and id, but has the list of contexts as its "children"
      JsonObject tmp = metricContexts.toJson();
      metricNode.add("contexts", tmp.getAsJsonArray("children"));
      output.add(metricNode);
    }

    return output;
  }

  private void addContext(String context, String metric, Map<String, ContextNode> metricContextsMap) {
    Iterator<String> contextParts = Splitter.on('.').split(context).iterator();
    if (!contextParts.hasNext()) {
      return;
    }
    String appId = contextParts.next();
    if (!contextParts.hasNext()) {
      return;
    }

    if (!metricContextsMap.containsKey(metric)) {
      metricContextsMap.put(metric, new ContextNode(ContextNodeType.ROOT, ""));
    }
    ContextNode metricContexts = metricContextsMap.get(metric);
    metricContexts = metricContexts.getOrAddChild(ContextNodeType.APP, appId);

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

  private class ContextNode implements Comparable<ContextNode> {
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

    public String getKey() {
      return type.name() + ":" + id;
    }

    private String getChildKey(ContextNodeType type, String id) {
      return type.name() + ":" + id;
    }

    public JsonObject toJson() {
      JsonObject output = new JsonObject();
      output.addProperty("type", type.getName());
      output.addProperty("id", id);
      // sort children by

      if (children.size() > 0) {
        List<ContextNode> childList = Lists.newArrayList(children.values());
        Collections.sort(childList);
        JsonArray childrenJson = new JsonArray();
        for (ContextNode child : childList) {
          childrenJson.add(child.toJson());
        }
        output.add("children", childrenJson);
      }
      return output;
    }

    public int compareTo(ContextNode other) {
      return getKey().compareTo(other.getKey());
    }
  }

}
