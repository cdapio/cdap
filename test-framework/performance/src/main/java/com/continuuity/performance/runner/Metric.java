package com.continuuity.performance.runner;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Class for metrics collected by performance test framework.
 */
public class Metric {

  /**
   * Defines different types of metrics that performance test framework uses.
   */
  public enum Type {
    TIME_SERIES,
    SUMMARY,
    AGGREGATE
  }

  /**
   * Defines different parameters that are used in REST URI to retrieve metric values.
   */
  public enum Parameter {
    APPLICATION_ID("appId"),
    FLOW_ID("flowId"),
    FLOWLET_ID("flowletId"),
    JOB_ID("jobId"),
    QUEUE_ID("queueId"),
    STREAM_ID("streamId"),
    DATASET_ID("datasetId"),
    PROCEDURE_ID("procedureId");

    private final String name;

    private Parameter(String name) {
      this.name = name;
    }
    public String getName() {
      return name;
    }
  }

  private final Type type;
  private final String path;
  private final ImmutableMap<Parameter, String> parameters;

  public Metric(String pattern, ImmutableMap<Parameter, String> parameters) {
    this.parameters = parameters;
    path = compilePath(pattern, parameters);
    type = findType(path);
  }

  public Metric(String path) {
    this.path = path;
    parameters = ImmutableMap.of();
    type = findType(path);
  }

  private static String compilePath(String pattern, ImmutableMap<Parameter, String> parameters) {
    String tempPath = pattern;
    for (Map.Entry<Parameter, String> entry : parameters.entrySet()) {
      tempPath = tempPath.replaceAll("\\{" + entry.getKey().getName() + "\\}", entry.getValue());
    }
    return tempPath;
  }

  private static Type findType(String path) {
    if (path.contains("?aggregate=true")) {
      return Type.AGGREGATE;
    } else if (path.contains("?summary=true")) {
      return Type.SUMMARY;
    } else {
      return Type.TIME_SERIES;
    }
  }

  public String getPath() {
    return path;
  }
  public Type getType() {
    return type;
  }
  public String getParameter(Parameter name) {
    return parameters.get(name);
  }
}
