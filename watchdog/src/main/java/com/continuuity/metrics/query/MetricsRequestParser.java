/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * For parsing metrics REST request.
 */
final class MetricsRequestParser {

  private static final String COUNT = "count";
  private static final String START_TIME = "start";
  private static final String END_TIME = "end";

  private enum ContextType {
    COLLECT,
    PROCESS,
    STORE,
    QUERY
  }

  private enum ProgramType {
    FLOWS("f"),
    MAPREDUCE("b"),
    PROCEDURES("p");

    private final String id;

    private ProgramType(String id) {
      this.id = id;
    }

    private String getId() {
      return id;
    }
  }

  private enum MapReduceType {
    MAPPERS("m"),
    REDUCERS("r");

    private final String id;

    private MapReduceType(String id) {
      this.id = id;
    }

    private String getId() {
      return id;
    }
  }

  /**
   * Parses the given uri into {@link MetricsRequest}.
   *
   * @throws IllegalArgumentException If the given uri is not a valid metrics request.
   */
  static MetricsRequest parse(URI requestURI) {
    Iterator<String> pathParts = Splitter.on('/').omitEmptyStrings().split(requestURI.getPath()).iterator();

    // 1. Context type
    ContextType contextType = ContextType.valueOf(pathParts.next().toUpperCase());

    // 2. Metric group (prefix)
    String metricName = pathParts.next();

    // Then, depending on the contextType, the parsing would be different.
    MetricsRequestBuilder builder = new MetricsRequestBuilder();

    switch (contextType) {
      case COLLECT:
        // TODO
        break;
      case PROCESS:
        parseProgram(metricName, pathParts, builder);
        break;
      case STORE:
        // TODO
        break;
      case QUERY:
        parseProgram(metricName, pathParts, builder);
        break;
    }

    // From the query string determine the query type and related parameters
    Map<String, List<String>> queryParams = new QueryStringDecoder(requestURI).getParameters();

    // Extra the query type.
    if (queryParams.containsKey(COUNT)) {
      try {
        builder.setCount(Integer.parseInt(queryParams.get(COUNT).get(0)));
        builder.setStartTime(queryParams.containsKey(START_TIME)
                               ? Integer.parseInt(queryParams.get(START_TIME).get(0))
                               : 0);
        builder.setEndTime(queryParams.containsKey(END_TIME)
                               ? Integer.parseInt(queryParams.get(END_TIME).get(0))
                               : TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS));
      } catch (Exception e) {
        throw new IllegalArgumentException(e);
      }
    } else {
      boolean foundType = false;
      for (MetricsRequest.Type type : MetricsRequest.Type.values()) {
        if (Boolean.parseBoolean(getQueryParam(queryParams, type.name().toLowerCase(), "false"))) {
          builder.setType(type);
          foundType = true;
          break;
        }
      }

      Preconditions.checkArgument(foundType, "Unknown query type for %s.", requestURI);
    }

    return builder.build();
  }


  /**
   * Parses metrics request for program type (process or query).
   */
  private static MetricsRequestBuilder parseProgram(String metricName,
                                                    Iterator<String> pathParts, MetricsRequestBuilder builder) {

    builder.setMetricPrefix(metricName);

    // 3. Application Id.
    String contextPrefix = pathParts.next();

    // 4. Optional program type
    ProgramType programType = pathParts.hasNext() ? ProgramType.valueOf(pathParts.next().toUpperCase()) : null;

    if (programType == null) {
      // Metrics for the application.
      return builder.setContextPrefix(contextPrefix);
    }

    contextPrefix += "." + programType.getId();

    // 5. Program ID
    contextPrefix += "." + pathParts.next();

    if (!pathParts.hasNext()) {
      // Metrics for the program.
      return builder.setContextPrefix(contextPrefix);
    }

    // 6. RunId for Map Reduce job or flowlet Id.
    if (programType == ProgramType.MAPREDUCE) {
      builder.setRunId(pathParts.next());
    } else {
      // flowlet Id
      contextPrefix += "." + pathParts.next();
    }

    if (!pathParts.hasNext()) {
      // Metrics for a given map reduce run or a flowlet
      return builder.setContextPrefix(contextPrefix);
    }

    // 7. Subtype for jobs program type.
    if (programType == ProgramType.MAPREDUCE) {
      contextPrefix += "." + MapReduceType.valueOf(pathParts.next().toUpperCase()).getId();

      if (pathParts.hasNext()) {
        // The mapper or reducer id
        contextPrefix += "." + pathParts.next();
      }
    } else {
      // It gives the suffix of the metric
      while (pathParts.hasNext()) {
        metricName += "." + pathParts.next();
      }
    }

    builder.setContextPrefix(contextPrefix);
    return builder.setMetricPrefix(metricName);
  }


  /**
   * Gets a query string parameter by the given key. It will returns the first value if available or the default value
   * if it is absent.
   */
  private static String getQueryParam(Map<String, List<String>> queries, String key, String defaultValue) {
    if (!queries.containsKey(key)) {
      return defaultValue;
    }
    List<String> values = queries.get(key);
    if (values.isEmpty()) {
      return defaultValue;
    }
    return values.get(0);
  }
}
