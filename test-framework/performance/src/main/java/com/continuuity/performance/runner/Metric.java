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
    TIME_SERIES(""),
    SUMMARY("?summary=true"),
    AGGREGATE("?aggregate=true");

    private final String restParameter;

    private Type(String restParameter) {
      this.restParameter = restParameter;
    }
    public String getParameter() {
      return restParameter;
    }
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

  /**
   * Constructs a new metric based on a pattern, a type and parameters.
   * @param pattern Pattern with place holders for parameters
   * @param type Type of metric
   * @param parameters Map of parameters to be used for compiling pattern to a full path
   */
  public Metric(String pattern, Type type, ImmutableMap<Parameter, String> parameters) {
    this.type = type;
    this.parameters = parameters;
    path = compilePath(pattern, type, parameters);
  }

  /**
   * Constructs a new metric based on a complete path that has a type as well as parameters already included.
   * @param completePath Complete path of metric
   */
  public Metric(String completePath) {
    type = findType(completePath);
    path = completePath;
    parameters = ImmutableMap.of();
  }

  /**
   * Constructs a new metric based on a path that has parameters already included but not the metric type.
   * @param path Path of metric
   * @param type Type of metric
   */
  public Metric(String path, Type type) {
    this.type = type;
    this.path = path + type.getParameter();
    parameters = ImmutableMap.of();
  }

  // Compiles the path of the metric based on a provided pattern, a type and a map with parameters
  private static String compilePath(String pattern, Type type, ImmutableMap<Parameter, String> parameters) {
    String tempPath = pattern;
    for (Map.Entry<Parameter, String> entry : parameters.entrySet()) {
      tempPath = tempPath.replaceAll("\\{" + entry.getKey().getName() + "\\}", entry.getValue());
    }
    return tempPath + type.getParameter();
  }

  // Searches for the metric type in the metric path
  private static Type findType(String path) {
    if (path.contains(Type.AGGREGATE.getParameter())) {
      return Type.AGGREGATE;
    } else if (path.contains(Type.SUMMARY.getParameter())) {
      return Type.SUMMARY;
    } else {
      return Type.TIME_SERIES;
    }
  }

  /**
   * Returns the compiled path of the metric.
   * @return String
   */
  public String getPath() {
    return path;
  }

  /**
   * Returns the type of the metric.
   * @return Type
   */
  public Type getType() {
    return type;
  }

  /**
   * Returns the value for a provided parameter name.
   * @return String
   */
  public String getParameter(Parameter name) {
    return parameters.get(name);
  }
}
