/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.metrics.query;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.metrics.MetricsScope;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.common.utils.TimeMathParser;
import com.continuuity.metrics.MetricsConstants;
import com.continuuity.metrics.data.Interpolator;
import com.continuuity.metrics.data.Interpolators;
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
  private static final String TRANSACTION_METRICS_CONTEXT = "transactions";

  public enum PathType {
    APPS,
    DATASETS,
    STREAMS,
    CLUSTER,
    SERVICES;
  }

  public enum RequestType {
    FLOWS("f"),
    MAPREDUCE("b"),
    PROCEDURES("p"),
    HANDLERS("h"),
    SERVICES("s");

    private final String code;

    private RequestType(String code) {
      this.code = code;
    }

    public String getCode() {
      return code;
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
      throw new IllegalArgumentException("unsupported encoding in path element", e);
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
    // +8 for "/metrics"
    int startPos = Constants.Gateway.GATEWAY_VERSION.length() + 8;
    return path.substring(startPos, path.length());
  }

  static MetricsRequest parse(URI requestURI) throws MetricsPathException {
    return parseRequestAndContext(requestURI).getFirst();
  }

  static ImmutablePair<MetricsRequest, MetricsRequestContext> parseRequestAndContext(URI requestURI)
    throws MetricsPathException {
    MetricsRequestBuilder builder = new MetricsRequestBuilder(requestURI);

    // metric will be at the end.
    String uriPath = requestURI.getRawPath();
    int index = uriPath.lastIndexOf("/");
    builder.setMetricPrefix(urlDecode(uriPath.substring(index + 1)));

    // strip the metric from the end of the path
    String strippedPath = uriPath.substring(0, index);

    MetricsRequestContext metricsRequestContext;
    if (strippedPath.startsWith("/reactor/cluster")) {
      builder.setContextPrefix(CLUSTER_METRICS_CONTEXT);
      builder.setScope(MetricsScope.REACTOR);
      metricsRequestContext = new MetricsRequestContext.Builder().build();
    } else if (strippedPath.startsWith("/reactor/transactions")) {
      builder.setContextPrefix(TRANSACTION_METRICS_CONTEXT);
      builder.setScope(MetricsScope.REACTOR);
      metricsRequestContext = new MetricsRequestContext.Builder().build();
    } else {
      metricsRequestContext = parseContext(strippedPath, builder);
    }
    parseQueryString(requestURI, builder);
    return new ImmutablePair<MetricsRequest, MetricsRequestContext>(builder.build(), metricsRequestContext);
  }

  /**
   * Parse the context path, setting the relevant context fields in the builder.
   * Context starts after the scope and looks something like:
   * reactor/apps/{app-id}/{program-type}/{program-id}/{component-type}/{component-id}
   */
  static MetricsRequestContext parseContext(String path, MetricsRequestBuilder builder) throws MetricsPathException {
    Iterator<String> pathParts = Splitter.on('/').omitEmptyStrings().split(path).iterator();
    MetricsRequestContext.Builder contextBuilder = new MetricsRequestContext.Builder();

    // scope is the first part of the path
    String scopeStr = pathParts.next();
    try {
      builder.setScope(MetricsScope.valueOf(scopeStr.toUpperCase()));
    } catch (IllegalArgumentException e) {
      throw new MetricsPathException("invalid scope: " + scopeStr);
    }

    // streams, datasets, apps, or nothing.
    if (!pathParts.hasNext()) {
      return contextBuilder.build();
    }

    // apps, streams, or datasets
    String pathTypeStr = pathParts.next();
    PathType pathType;
    try {
      pathType = PathType.valueOf(pathTypeStr.toUpperCase());
      contextBuilder.setPathType(pathType);
    } catch (IllegalArgumentException e) {
      throw new MetricsPathException("invalid type: " + pathTypeStr);
    }

    switch(pathType) {
      case APPS:
        parseSubContext(pathParts, contextBuilder);
        break;
      case STREAMS:
        if (!pathParts.hasNext()) {
          throw new MetricsPathException("'streams' must be followed by a stream name");
        }
        contextBuilder.setTag(MetricsRequestContext.TagType.STREAM, urlDecode(pathParts.next()));
        break;
      case DATASETS:
        if (!pathParts.hasNext()) {
          throw new MetricsPathException("'datasets' must be followed by a dataset name");
        }
        contextBuilder.setTag(MetricsRequestContext.TagType.DATASET, urlDecode(pathParts.next()));
        // path can be /metric/scope/datasets/{dataset}/apps/...
        if (pathParts.hasNext()) {
          if (!pathParts.next().equals("apps")) {
            throw new MetricsPathException("expecting 'apps' after stream or dataset name");
          }
          parseSubContext(pathParts, contextBuilder);
        }
        break;
      case SERVICES:
        if (!pathParts.hasNext()) {
          throw new MetricsPathException("'services must be followed by a service name");
        }
        parseSubContext(pathParts, contextBuilder);
        break;
    }

    if (pathParts.hasNext()) {
      throw new MetricsPathException("path contains too many elements");
    }
    MetricsRequestContext context = contextBuilder.build();
    builder.setContextPrefix(context.getContextPrefix());
    if (context.getTag() != null) {
      builder.setTagPrefix(context.getTag());
    }
    return context;
  }

  /**
   * pathParts should look like {app-id}/{program-type}/{program-id}/{component-type}/{component-id}.
   */
  static void parseSubContext(Iterator<String> pathParts, MetricsRequestContext.Builder builder)
    throws MetricsPathException {

    if (!pathParts.hasNext()) {
      return;
    }
    builder.setTypeId(urlDecode(pathParts.next()));

    if (!pathParts.hasNext()) {
      return;
    }

    // request-type: flows, procedures, or mapreduce or handlers or services(user)
    String pathProgramTypeStr = pathParts.next();
    RequestType requestType;
    try {
      requestType = RequestType.valueOf(pathProgramTypeStr.toUpperCase());
      builder.setRequestType(requestType);
    } catch (IllegalArgumentException e) {
      throw new MetricsPathException("invalid program type: " + pathProgramTypeStr);
    }

    // contextPrefix should look like appId.f right now, if we're looking at a flow
    if (!pathParts.hasNext()) {
      return;
    }
    builder.setRequestId(urlDecode(pathParts.next()));

    if (!pathParts.hasNext()) {
      return;
    }

    switch(requestType) {
      case MAPREDUCE:
        String mrTypeStr = pathParts.next();
        MapReduceType mrType;
        try {
          mrType = MapReduceType.valueOf(mrTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
          throw new MetricsPathException("invalid mapreduce component: " + mrTypeStr
                                           + ".  must be 'mappers' or 'reducers'.");
        }
        builder.setComponentId(mrType.getId());
        break;
      case FLOWS:
        buildFlowletContext(pathParts, builder);
        break;
      case HANDLERS:
        buildHandlerContext(pathParts, builder);
        break;
      case SERVICES:
        buildUserServiceContext(pathParts, builder);
    }

    if (pathParts.hasNext()) {
      throw new MetricsPathException("path contains too many elements");
    }
  }

  private static void buildUserServiceContext(Iterator<String> pathParts, MetricsRequestContext.Builder builder)
    throws MetricsPathException {
    if (!pathParts.next().equals("runnables")) {
      throw new MetricsPathException("expecting 'runnables' after the service name");
    }
    if (!pathParts.hasNext()) {
      throw new MetricsPathException("runnables must be followed by a runnable name");
    }
    builder.setComponentId(urlDecode(pathParts.next()));
  }


  /**
   * At this point, pathParts should look like methods/{method-name}
   */
  private static void buildHandlerContext(Iterator<String> pathParts, MetricsRequestContext.Builder builder)
    throws MetricsPathException {
    if (!pathParts.next().equals("methods")) {
      throw new MetricsPathException("expecting 'methods' after the handler name");
    }
    if (!pathParts.hasNext()) {
      throw new MetricsPathException("methods must be followed by a method name");
    }
    builder.setComponentId(urlDecode(pathParts.next()));
  }

  /**
   * At this point, pathParts should look like flowlets/{flowlet-id}/queues/{queue-id}, with queues being optional.
   */
  private static void buildFlowletContext(Iterator<String> pathParts, MetricsRequestContext.Builder builder)
    throws MetricsPathException {
    if (!pathParts.next().equals("flowlets")) {
      throw new MetricsPathException("expecting 'flowlets' after the flow name");
    }
    if (!pathParts.hasNext()) {
      throw new MetricsPathException("flowlets must be followed by a flowlet name");
    }
    builder.setComponentId(urlDecode(pathParts.next()));

    if (pathParts.hasNext()) {
      if (!pathParts.next().equals("queues")) {
        throw new MetricsPathException("expecting 'queues' after the flowlet name");
      }
      if (!pathParts.hasNext()) {
        throw new MetricsPathException("'queues' must be followed by a queue name");
      }
      builder.setTag(MetricsRequestContext.TagType.QUEUE, urlDecode(pathParts.next()));
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

      if (!foundType) {
        throw new IllegalArgumentException("Unknown query type for " + requestURI);
      }
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
