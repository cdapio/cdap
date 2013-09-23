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

  private enum PathType {
    APPS,
    DATASETS,
    STREAMS
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

  private static String urlDecode(String str) {
    try {
      return URLDecoder.decode(str, CharEncoding.UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("Unsupported encoding in path element", e);
    }
  }

  /**
   * Parses the given uri into {@link MetricsRequest}.
   *
   * @throws IllegalArgumentException If the given uri is not a valid metrics request.
   */
  static MetricsRequest parse(URI requestURI) {
    MetricsRequestBuilder builder = new MetricsRequestBuilder(requestURI);

    // metric will be at the end.
    String uriPath = requestURI.getRawPath();
    int index = uriPath.lastIndexOf("/");
    builder.setMetricPrefix(urlDecode(uriPath.substring(index + 1)));

    // strip the metric from the end of the path
    String strippedPath = uriPath.substring(0, index);
    Iterator<String> pathParts = Splitter.on('/').omitEmptyStrings().split(strippedPath).iterator();

    // Scope
    builder.setScope(MetricsScope.valueOf(pathParts.next().toUpperCase()));

    // streams, datasets, apps, or nothing.
    if (!pathParts.hasNext()) {
      // null context means the context can be anything
      builder.setContextPrefix(null);
      parseQueryString(requestURI, builder);
      return builder.build();
    }

    // apps, streams, or datasets
    PathType pathType = PathType.valueOf(pathParts.next().toUpperCase());
    switch(pathType) {
      case APPS:
        buildAppContext(pathParts, builder);
        break;
      case STREAMS:
      case DATASETS:
        builder.setTagPrefix(urlDecode(pathParts.next()));
        // path can be /metric/scope/datasets/{dataset}/apps/...
        if (pathParts.hasNext()) {
          Preconditions.checkArgument(pathParts.next().equals("apps"), "expecting 'apps' after stream or dataset id");
          buildAppContext(pathParts, builder);
        } else {
          // path can also just be /metric/scope/datasets/{dataset}
          builder.setContextPrefix(null);
        }
        break;
    }

    parseQueryString(requestURI, builder);
    return builder.build();
  }

  /**
   * At this point, pathParts should look like {app-id}/{program-type}/{program-id}/{component-type}/{component-id}
   */
  private static void buildAppContext(Iterator<String> pathParts, MetricsRequestBuilder builder) {
    String contextPrefix = urlDecode(pathParts.next());

    if (!pathParts.hasNext()) {
      builder.setContextPrefix(contextPrefix);
      return;
    }

    // program-type, flows, procedures, or mapreduce
    ProgramType programType = ProgramType.valueOf(pathParts.next().toUpperCase());
    contextPrefix += "." + programType.getId();

    // contextPrefix should look like appId.f right now, if we're looking at a flow
    if (!pathParts.hasNext()) {
      builder.setContextPrefix(contextPrefix);
      return;
    }
    contextPrefix += "." + urlDecode(pathParts.next());

    if (!pathParts.hasNext()) {
      builder.setContextPrefix(contextPrefix);
      return;
    }

    switch(programType) {
      case MAPREDUCE:
        buildMapReduceContext(contextPrefix, pathParts, builder);
        break;
      case FLOWS:
        buildFlowletContext(contextPrefix, pathParts, builder);
        break;
      case PROCEDURES:
        throw new IllegalArgumentException("invalid path: not expecting anything after procedure-id");
    }
  }


  /**
   *At this point, pathParts should look like {mappers | reducers}/{optional id}
   */
  private static void buildMapReduceContext(String contextPrefix, Iterator<String> pathParts,
                                            MetricsRequestBuilder builder) {
    MapReduceType mrType = MapReduceType.valueOf(pathParts.next().toUpperCase());
    contextPrefix += "." + mrType.getId();
    if (pathParts.hasNext()) {
      contextPrefix += "." + pathParts.next();
      Preconditions.checkArgument(!pathParts.hasNext(), "not expecting anything after mapper or reducer id");
    }
    builder.setContextPrefix(contextPrefix);
  }

  /**
   * At this point, pathParts should look like flowlets/{flowlet-id}/queues/{queue-id}, with queues being optional
   */
  private static void buildFlowletContext(String contextPrefix, Iterator<String> pathParts,
                                          MetricsRequestBuilder builder) {
    Preconditions.checkArgument(pathParts.next().equals("flowlets"), "expecting 'flowlets' after flow id");
    contextPrefix += "." + urlDecode(pathParts.next());
    builder.setContextPrefix(contextPrefix);

    if (pathParts.hasNext()) {
      Preconditions.checkArgument(pathParts.next().equals("queues"), "expecting 'queues' after flowlet id");
      builder.setTagPrefix(pathParts.next());
    }
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
