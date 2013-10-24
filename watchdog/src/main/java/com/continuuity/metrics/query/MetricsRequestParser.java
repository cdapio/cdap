/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.common.utils.TimeMathParser;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.data.Interpolator;
import com.continuuity.metrics.data.Interpolators;
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
  private static final String INTERPOLATE = "interpolate";
  private static final String STEP_INTERPOLATOR = "step";
  private static final String LINEAR_INTERPOLATOR = "linear";
  private static final String MAX_INTERPOLATE_GAP = "maxInterpolateGap";
  private static final String CLUSTER_METRICS_CONTEXT = "-.cluster";
  private static final String CONTEXT_SEPARATOR = ".";


  private enum PathType {
    APPS,
    DATASETS,
    STREAMS,
    CLUSTER
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

    parseContext(strippedPath, builder);
    parseQueryString(requestURI, builder);
    return builder.build();
  }

  static void parseContext(String contextPath, MetricsRequestBuilder builder) {
    Iterator<String> pathParts = Splitter.on('/').omitEmptyStrings().split(contextPath).iterator();

    // Scope
    builder.setScope(MetricsScope.valueOf(pathParts.next().toUpperCase()));

    // streams, datasets, apps, or nothing.
    if (!pathParts.hasNext()) {
      // null context means the context can be anything
      builder.setContextPrefix(null);
      return;
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
      case CLUSTER:
        builder.setContextPrefix(CLUSTER_METRICS_CONTEXT);
        break;
    }
  }

  /**
   * Given a full metrics path like '/v2/metrics/reactor/apps/collect.events', strip the preceding version and
   * metrics to return 'reactor/apps/collect.events', representing the context and metric, which can then be
   * parsed by this parser.
   *
   * @param path request path.
   * @return request path stripped of version and metrics.
   */
  static String stripVersionAndMetricsFromPath(String path) {
    // +9 for "/metrics/"
    int startPos = Constants.Gateway.GATEWAY_VERSION.length() + 9;
    return path.substring(startPos, path.length());
  }

  /**
   * At this point, pathParts should look like {app-id}/{program-type}/{program-id}/{component-type}/{component-id}.
   */
  private static void buildAppContext(Iterator<String> pathParts, MetricsRequestBuilder builder) {
    String contextPrefix = urlDecode(pathParts.next());

    if (!pathParts.hasNext()) {
      builder.setContextPrefix(contextPrefix);
      return;
    }

    // program-type, flows, procedures, or mapreduce
    ProgramType programType = ProgramType.valueOf(pathParts.next().toUpperCase());
    contextPrefix += CONTEXT_SEPARATOR + programType.getId();

    // contextPrefix should look like appId.f right now, if we're looking at a flow
    if (!pathParts.hasNext()) {
      builder.setContextPrefix(contextPrefix);
      return;
    }
    contextPrefix += CONTEXT_SEPARATOR + urlDecode(pathParts.next());

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
   * At this point, pathParts should look like {mappers | reducers}/{optional id}.
   */
  private static void buildMapReduceContext(String contextPrefix, Iterator<String> pathParts,
                                            MetricsRequestBuilder builder) {
    MapReduceType mrType = MapReduceType.valueOf(pathParts.next().toUpperCase());
    contextPrefix += CONTEXT_SEPARATOR + mrType.getId();
    if (pathParts.hasNext()) {
      contextPrefix += CONTEXT_SEPARATOR + pathParts.next();
      Preconditions.checkArgument(!pathParts.hasNext(), "not expecting anything after mapper or reducer id");
    }
    builder.setContextPrefix(contextPrefix);
  }

  /**
   * At this point, pathParts should look like flowlets/{flowlet-id}/queues/{queue-id}, with queues being optional.
   */
  private static void buildFlowletContext(String contextPrefix, Iterator<String> pathParts,
                                          MetricsRequestBuilder builder) {
    Preconditions.checkArgument(pathParts.next().equals("flowlets"), "expecting 'flowlets' after flow id");
    contextPrefix += CONTEXT_SEPARATOR + urlDecode(pathParts.next());
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
    if (isTimeseriesRequest(queryParams)) {
      parseTimeseries(queryParams, builder);
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

  private static boolean isTimeseriesRequest(Map<String, List<String>> queryParams) {
    return queryParams.containsKey(COUNT) || queryParams.containsKey(START_TIME) || queryParams.containsKey(END_TIME);
  }

  private static void parseTimeseries(Map<String, List<String>> queryParams, MetricsRequestBuilder builder) {
    int count;
    long startTime;
    long endTime;
    long now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    if (queryParams.containsKey(START_TIME) && queryParams.containsKey(END_TIME)) {
      startTime = TimeMathParser.parseTime(now, queryParams.get(START_TIME).get(0));
      endTime = TimeMathParser.parseTime(now, queryParams.get(END_TIME).get(0));
      count = (int) (endTime - startTime) + 1;
    } else if (queryParams.containsKey(COUNT)) {
      count = Integer.parseInt(queryParams.get(COUNT).get(0));
      // both start and end times are inclusive, which is the reason for the +-1.
      if (queryParams.containsKey(START_TIME)) {
        startTime = TimeMathParser.parseTime(now, queryParams.get(START_TIME).get(0));
        endTime = startTime + count - 1;
      } else if (queryParams.containsKey(END_TIME)) {
        endTime = TimeMathParser.parseTime(now, queryParams.get(END_TIME).get(0));
        startTime = endTime - count + 1;
      } else {
        // if only count is specified, assume the current time is desired as the end.
        endTime = now - MetricsConstants.QUERY_SECOND_DELAY;
        startTime = endTime - count + 1;
      }
    } else {
      throw new IllegalArgumentException("must specify 'count', or both 'start' and 'end'");
    }

    builder.setStartTime(startTime);
    builder.setEndTime(endTime);
    builder.setCount(count);
    builder.setType(MetricsRequest.Type.TIME_SERIES);
    setInterpolator(queryParams, builder);
  }

  private static void setInterpolator(Map<String, List<String>> queryParams, MetricsRequestBuilder builder) {
    Interpolator interpolator = null;

    if (queryParams.containsKey(INTERPOLATE)) {
      String interpolatorType = queryParams.get(INTERPOLATE).get(0);
      // timeLimit used in case there is a big gap in the data and we don't want to interpolate points.
      // the limit defines how big the gap has to be in seconds before we just say they're all zeroes.
      long timeLimit = queryParams.containsKey(MAX_INTERPOLATE_GAP)
        ? Long.parseLong(queryParams.get(MAX_INTERPOLATE_GAP).get(0))
        : Long.MAX_VALUE;

      if (STEP_INTERPOLATOR.equals(interpolatorType)) {
        interpolator = new Interpolators.Step(timeLimit);
      } else if (LINEAR_INTERPOLATOR.equals(interpolatorType)) {
        interpolator = new Interpolators.Linear(timeLimit);
      }
    }
    builder.setInterpolator(interpolator);
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
