/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.metrics.MetricsConstants;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.commons.lang.CharEncoding;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
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

  // Events path parse is slightly different. If there is no "ins" or "outs", it's default to "processed".
  private static final String EVENTS = "process.events";
  private static final String PROCESSED = "processed";

  private enum ContextType {
    COLLECT,
    PROCESS,
    STORE,
    QUERY,
    USER;
  }

  private enum ProgramType {
    FLOWS("f"),
    MAPREDUCES("b"),
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

  private enum StoreType {
    APPS,
    DATASETS;
  }

  private enum CollectType {
    APPS,
    STREAMS;
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
    String metricName = contextType.name().toLowerCase() + "." + pathParts.next();

    MetricsRequestBuilder builder = new MetricsRequestBuilder(requestURI);
    builder.setMetricPrefix(metricName);

    if (pathParts.hasNext()) {
      // Then, depending on the contextType, the parsing would be different.
      switch (contextType) {
        case COLLECT:
          parseCollect(pathParts, builder);
          break;
        case PROCESS:
          parseProgram(metricName, pathParts, builder);
          break;
        case STORE:
          parseStore(pathParts, builder);
          break;
        case QUERY:
          parseProgram(metricName, pathParts, builder);
          break;
        case USER:
          parseUser(requestURI.getRawPath(), builder);
          break;
      }
    }
    builder.setScope((contextType == ContextType.USER) ? MetricsScope.USER : MetricsScope.REACTOR);

    parseQueryString(requestURI, builder);

    return builder.build();
  }

  private static MetricsRequestBuilder parseCollect(Iterator<String> pathParts, MetricsRequestBuilder builder) {
    // 3. Collection type
    CollectType collectType = CollectType.valueOf(pathParts.next().toUpperCase());

    if (collectType == CollectType.APPS) {
      // 4. If it is apps, then it's appId
      return builder.setContextPrefix(pathParts.next());
    }

    // 5. Stream Id
    builder.setTagPrefix(pathParts.next());

    if (!pathParts.hasNext()) {
      // If no more parts, it's the metric for the whole stream
      return builder;
    }

    // 6. App ID
    String context = pathParts.next();

    // 7. Program type
    context += "." + ProgramType.valueOf(pathParts.next().toUpperCase()).getId();

    // 7. Program ID
    context += "." + pathParts.next();

    return builder.setContextPrefix(context);
  }

  /**
   * Parses metrics request for user metrics, where pathParts is an iterator over the path of form:
   * /user/apps/{appid}/{programType}/{programId}/{componentId}/metricname
   * where everything between the appid and metric name are optional.
   */
  private static MetricsRequestBuilder parseUser(String uriPath, MetricsRequestBuilder builder) {
    // getting the metric from the end...
    int index = uriPath.lastIndexOf("/");
    String metricName = uriPath.substring(index + 1);

    try {
      metricName = URLDecoder.decode(metricName, CharEncoding.UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("metric name uses an unsupported encoding");
    }

    String strippedPath = uriPath.substring(0, index);
    Iterator<String> pathParts = Splitter.on('/').omitEmptyStrings().split(strippedPath).iterator();
    pathParts.next();
    pathParts.next();
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

    if (!pathParts.hasNext()) {
      return builder.setContextPrefix(contextPrefix);
    }
    // 5. Program ID
    contextPrefix += "." + pathParts.next();

    if (!pathParts.hasNext()) {
      // Metrics for the program.
      return builder.setContextPrefix(contextPrefix);
    }

    // 6. Subtype ("mappers/reducers") for Map Reduce job or flowlet Id.
    if (programType == ProgramType.MAPREDUCES) {
      contextPrefix += "." + MapReduceType.valueOf(pathParts.next().toUpperCase()).getId();
    } else {
      // flowlet Id
      contextPrefix += "." + pathParts.next();
    }

    if (!pathParts.hasNext()) {
      // Metrics for the program component.
      return builder.setContextPrefix(contextPrefix);
    }

    // 7. Subtype ID for map reduce job or flowlet metric suffix
    if (programType == ProgramType.MAPREDUCES) {
      contextPrefix += "." + pathParts.next();
    }

    return builder.setContextPrefix(contextPrefix);
  }

  /**
   * Parses metrics request for program type.
   */
  private static MetricsRequestBuilder parseProgram(String metricName,
                                                    Iterator<String> pathParts, MetricsRequestBuilder builder) {
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
      if (EVENTS.equals(metricName)) {    // Special handling for events
        builder.setMetricPrefix(metricName + "." + PROCESSED);
      }
      return builder.setContextPrefix(contextPrefix);
    }

    // 6. Subtype ("mappers/reducers") for Map Reduce job or flowlet Id.
    if (programType == ProgramType.MAPREDUCES) {
      contextPrefix += "." + MapReduceType.valueOf(pathParts.next().toUpperCase()).getId();
    } else {
      // flowlet Id
      contextPrefix += "." + pathParts.next();
    }

    if (!pathParts.hasNext()) {
      // Metrics for the program component.
      if (EVENTS.equals(metricName)) {    // Special handling for events
        builder.setMetricPrefix(metricName + "." + PROCESSED);
      }
      return builder.setContextPrefix(contextPrefix);
    }

    // 7. Subtype ID for map reduce job or flowlet metric suffix
    if (programType == ProgramType.MAPREDUCES) {
      contextPrefix += "." + pathParts.next();
    } else {
      // It gives the suffix of the metric
      while (pathParts.hasNext()) {
        metricName += "." + pathParts.next();
      }
    }

    if (EVENTS.equals(metricName)) {
      metricName += "." + PROCESSED;
    }

    return builder.setContextPrefix(contextPrefix)
                  .setMetricPrefix(metricName);
  }

  private static MetricsRequestBuilder parseStore(Iterator<String> pathParts, MetricsRequestBuilder builder) {
    // 3. Either "apps" or "datasets"
    StoreType storeType = StoreType.valueOf(pathParts.next().toUpperCase());

    // 4. The id, based on the storeType, it could be AppId or DatasetId
    String id = pathParts.next();

    if (!pathParts.hasNext()) {
      return storeType == StoreType.APPS ? builder.setContextPrefix(id) : builder.setTagPrefix(id);
    }

    // If it is "apps" type query
    if (storeType == StoreType.APPS) {
      String context = id;
      // 5. It must be one of the program type
      ProgramType programType = ProgramType.valueOf(pathParts.next().toUpperCase());
      context += "." + programType.getId();

      // 6. Next is the program ID
      context += "." + pathParts.next();

      if (!pathParts.hasNext()) {
        // If nothing more, it's the program metrics for all datasets
        return builder.setContextPrefix(context);
      }

      if (programType == ProgramType.FLOWS) {
        // 7. Flowlet ID
        context += "." + pathParts.next();
        if (!pathParts.hasNext()) {
          // If nothing more, it's the flowlet metrics for all datasets
          return builder.setContextPrefix(context);
        }
      }

      // 7/8. It must be "datasets"
      Preconditions.checkArgument(StoreType.valueOf(pathParts.next().toUpperCase()) == StoreType.DATASETS,
                                  "It must be \"%s\".", StoreType.DATASETS.name().toLowerCase());

      return builder.setContextPrefix(context)
                    .setTagPrefix(pathParts.next());
    }

    // Otherwise, it is "datasets" type query.

    builder.setTagPrefix(id);

    // 5. Next is appID
    String context = pathParts.next();

    // 6. Program type
    context += "." + ProgramType.valueOf(pathParts.next().toUpperCase()).getId();

    // 7. Program ID
    context += "." + pathParts.next();

    return builder.setContextPrefix(context);
  }

  /**
   * From the query string determine the query type and related parameters.
   */
  private static void parseQueryString(URI requestURI, MetricsRequestBuilder builder) {

    Map<String, List<String>> queryParams = new QueryStringDecoder(requestURI).getParameters();

    // Extracts the query type.
    if (queryParams.containsKey(COUNT)) {
      try {
        int count = Integer.parseInt(queryParams.get(COUNT).get(0));
        long endTime = queryParams.containsKey(END_TIME)
                          ? Integer.parseInt(queryParams.get(END_TIME).get(0))
                          : TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS) -
                                MetricsConstants.QUERY_SECOND_DELAY;

        long startTime = queryParams.containsKey(START_TIME)
                          ? Integer.parseInt(queryParams.get(START_TIME).get(0))
                          : endTime - count;

        if (startTime + count != endTime) {
          endTime = startTime + count;
        }

        builder.setStartTime(startTime);
        builder.setEndTime(endTime);
        builder.setCount(count);
        builder.setType(MetricsRequest.Type.TIME_SERIES);
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
