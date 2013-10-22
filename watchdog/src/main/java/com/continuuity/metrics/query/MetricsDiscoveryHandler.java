/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HandlerContext;
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
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Class for handling requests for aggregate application metrics of the
 * {@link com.continuuity.common.metrics.MetricsScope#USER} scope.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public final class MetricsDiscoveryHandler extends AbstractHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MetricsDiscoveryHandler.class);

  private final Map<MetricsScope, AggregatesTable> aggregatesTables;

  // just user metrics for now.  Can add reactor metrics when there is a unified way to query for them
  // currently you query differently depending on the metric, and some metrics you can query for in the
  // BatchMetricsHandler are computed in the handler and are not stored in the table.
  private final MetricsScope[] scopesToDiscover = {MetricsScope.USER};

  // known 'program types' in a metric context (app.programType.programId.componentId)
  private enum ProgramType {
    PROCEDURES("p"),
    MAPREDUCE("b"),
    FLOWS("f"),
    STREAMS("stream"),
    DATASETS("dataset"),
    UNKNOWN("");
    String id;

    private ProgramType(String id) {
      this.id = id;
    }

    private String getId() {
      return id;
    }

    private static ProgramType fromId(String id) {
      for (ProgramType p : ProgramType.values()) {
        if (p.id.equals(id)) {
          return p;
        }
      }
      LOG.debug("unknown program type: " + id);
      return UNKNOWN;
    }
  }

  private enum ComponentType {
    FLOWLETS,
    MAPPERS,
    REDUCERS;
  }

  // used to display the type of a context node in the response
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

  // the context has 'm' and 'r', but they're referred to as 'mappers' and 'reducers' in the api.
  private enum MapReduceTask {
    M("mappers"),
    R("reducers");

    private final String name;

    private MapReduceTask(String name) {
      this.name = name;
    }

    private String getName() {
      return name;
    }
  }

  @Inject
  public MetricsDiscoveryHandler(final MetricsTableFactory metricsTableFactory) {
    this.aggregatesTables = Maps.newHashMap();
    for (MetricsScope scope : scopesToDiscover) {
      aggregatesTables.put(scope, metricsTableFactory.createAggregates(scope.name()));
    }
  }

  @Override
  public void init(HandlerContext context) {
    super.init(context);
    LOG.info("Starting MetricsDiscoveryHandler");
  }

  @Override
  public void destroy(HandlerContext context) {
    super.destroy(context);
    LOG.info("Stopping MetricsDiscoveryHandler");
  }

  @GET
  @Path("/metrics/available")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder) throws IOException {
    responder.sendJson(HttpResponseStatus.OK, getMetrics(request, null));
  }

  // ex: /metrics/available/apps/appX
  @GET
  @Path("/metrics/available/apps/{app-id}")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId) throws IOException {
    responder.sendJson(HttpResponseStatus.OK, getMetrics(request, appId));
  }

  // ex: /metrics/available/apps/appX/flows
  @GET
  @Path("/metrics/available/apps/{app-id}/{program-type}")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId,
                                      @PathParam("program-type") String programType) throws IOException {
    String programTypeId = ProgramType.valueOf(programType.toUpperCase()).getId();
    String context = Joiner.on(".").join(appId, programTypeId);
    responder.sendJson(HttpResponseStatus.OK, getMetrics(request, context));
  }

  // ex: /metrics/available/apps/appX/flows/flowY
  @GET
  @Path("/metrics/available/apps/{app-id}/{program-type}/{program-id}")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId,
                                      @PathParam("program-type") String programType,
                                      @PathParam("program-id") String programId) throws IOException {
    String programTypeId = ProgramType.valueOf(programType.toUpperCase()).getId();
    String context = Joiner.on(".").join(appId, programTypeId, programId);
    responder.sendJson(HttpResponseStatus.OK, getMetrics(request, context));
  }

  // ex: /metrics/available/apps/appX/flows/flowY/flowlets/flowletZ
  @GET
  @Path("/metrics/available/apps/{app-id}/{program-type}/{program-id}/{component-type}/{component-id}")
  public void handleAppMetricsRequest(HttpRequest request, HttpResponder responder,
                                      @PathParam("app-id") String appId,
                                      @PathParam("program-type") String programType,
                                      @PathParam("program-id") String programId,
                                      @PathParam("component-type") String componentType,
                                      @PathParam("component-id") String componentId) throws IOException {
    ComponentType.valueOf(componentType.toUpperCase());
    String programTypeId = ProgramType.valueOf(programType.toUpperCase()).getId();
    String context = Joiner.on(".").join(appId, programTypeId, programId, componentId);
    responder.sendJson(HttpResponseStatus.OK, getMetrics(request, context));
  }

  private JsonArray getMetrics(HttpRequest request, String contextPrefix) {
    // TODO(albert): add ability to pass in maxAge through query params
    Map<String, List<String>> queryParams = new QueryStringDecoder(request.getUri()).getParameters();
    List<String> prefixEntity = queryParams.get("prefixEntity");
    // shouldn't be in params more than once, but if it is, just take any one
    String metricPrefix = (prefixEntity == null || prefixEntity.isEmpty()) ? null : prefixEntity.get(0);

    Map<String, ContextNode> metricContextsMap = Maps.newHashMap();
    for (AggregatesTable table : aggregatesTables.values()) {
      AggregatesScanner scanner = table.scanRowsOnly(contextPrefix, metricPrefix);

      // scanning through all metric rows in the aggregates table
      // row has context plus metric info
      // each metric can show up in multiple contexts
      while (scanner.hasNext()) {
        AggregatesScanResult result = scanner.next();
        addContext(result.getContext(), result.getMetric(), metricContextsMap);
      }
    }

    // return the metrics sorted by metric name so it can directly be displayed to the user.
    JsonArray output = new JsonArray();
    List<String> sortedMetrics = Lists.newArrayList(metricContextsMap.keySet());
    Collections.sort(sortedMetrics);
    for (String metric : sortedMetrics) {
      JsonObject metricNode = new JsonObject();
      metricNode.addProperty("metric", metric);
      ContextNode metricContexts = metricContextsMap.get(metric);
      // the root node has junk for its type and id, but has the list of contexts as its "children"
      metricNode.add("contexts", metricContexts.toJson().getAsJsonArray("children"));
      output.add(metricNode);
    }

    return output;
  }

  /**
   * Eventually we need to return the tree of contexts each metric belongs to, but we're not going to get
   * all the contexts for a metric in order.  This is to add a node to the context tree, where the tree
   * will end up looking something like:
   *
   *  "contexts":[
   *   {
   *     "type":"app",
   *     "id":"WordCount",
   *     "children":[
   *       {
   *         "type":"flow",
   *         "id":"WordCounter",
   *         "children":[
   *           {
   *             "type":"flowlet",
   *             "id":"Associator"
   *           },...
   *         ]
   *       },...
   *     ]
   *   },...
   * ]
   * @param context
   * @param metric
   * @param metricContextsMap
   */
  private void addContext(String context, String metric, Map<String, ContextNode> metricContextsMap) {
    Iterator<String> contextParts = Splitter.on('.').split(context).iterator();
    if (!contextParts.hasNext()) {
      return;
    }
    String appId = contextParts.next();
    if (!contextParts.hasNext()) {
      return;
    }

    // get the context tree for the metric
    if (!metricContextsMap.containsKey(metric)) {
      metricContextsMap.put(metric, new ContextNode(ContextNodeType.ROOT, ""));
    }
    ContextNode metricContexts = metricContextsMap.get(metric);
    metricContexts = metricContexts.getOrAddChild(ContextNodeType.APP, appId);

    // different program types will have different depths in the context tree.  For example,
    // procedures have no children, whereas flows will have flowlets as children.
    ProgramType type = ProgramType.fromId(contextParts.next());
    switch(type) {
      case FLOWS:
        metricContexts.deepAdd(contextParts, ContextNodeType.FLOW, ContextNodeType.FLOWLET);
        break;
      case PROCEDURES:
        metricContexts.deepAdd(contextParts, ContextNodeType.PROCEDURE);
        break;
      case MAPREDUCE:
        if (contextParts.hasNext()) {
          metricContexts = metricContexts.getOrAddChild(ContextNodeType.MAPREDUCE, contextParts.next());
          if (contextParts.hasNext()) {
            String taskStr = MapReduceTask.valueOf(contextParts.next().toUpperCase()).getName();
            metricContexts.addChild(new ContextNode(ContextNodeType.MAPREDUCE_TASK, taskStr));
          }
        }
        break;
      // dataset and stream are special cases, currently the only "context" for them is "-.dataset" and "-.stream"
      case DATASETS:
        metricContexts.addChild(new ContextNode(ContextNodeType.DATASET, ""));
        break;
      case STREAMS:
        metricContexts.addChild(new ContextNode(ContextNodeType.STREAM, ""));
        break;
      default:
        break;
    }
  }

  // Class for building up the context tree for a metric.
  private class ContextNode implements Comparable<ContextNode> {
    private final ContextNodeType type;
    private final String id;
    private final Map<String, ContextNode> children;

    public ContextNode(ContextNodeType type, String id) {
      this.type = type;
      this.id = id;
      this.children = Maps.newHashMap();
    }

    // get the specified child, creating and adding it if it doesn't already exist
    public ContextNode getOrAddChild(ContextNodeType type, String id) {
      String key = getChildKey(type, id);
      if (!children.containsKey(key)) {
        children.put(key, new ContextNode(type, id));
      }
      return children.get(key);
    }

    // recursively add children of children of the given ids and types
    public void deepAdd(Iterator<String> ids, ContextNodeType... types) {
      ContextNode node = this;
      for (ContextNodeType type : types) {
        if (!ids.hasNext()) {
          break;
        }
        node = node.getOrAddChild(type, ids.next());
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

    // convert to json to send back in a response.  Each node has a type and id, with
    // an optional array of children.
    public JsonObject toJson() {
      JsonObject output = new JsonObject();
      output.addProperty("type", type.getName());
      output.addProperty("id", id);

      if (children.size() > 0) {
        // maybe make the sort optional based on query params
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
