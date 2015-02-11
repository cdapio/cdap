/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.metrics.query;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.utils.TimeMathParser;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.data.Interpolator;
import co.cask.cdap.metrics.data.Interpolators;
import co.cask.cdap.metrics.store.cube.CubeDeleteQuery;
import co.cask.cdap.metrics.store.cube.CubeQuery;
import co.cask.cdap.metrics.store.timeseries.MeasureType;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.commons.lang.CharEncoding;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * For parsing metrics REST request.
 */
final class MetricQueryParser {

  private static final String COUNT = "count";
  private static final String START_TIME = "start";
  private static final String RESOLUTION = "resolution";
  private static final String END_TIME = "end";
  private static final String RUN_ID = "runs";
  private static final String INTERPOLATE = "interpolate";
  private static final String STEP_INTERPOLATOR = "step";
  private static final String LINEAR_INTERPOLATOR = "linear";
  private static final String MAX_INTERPOLATE_GAP = "maxInterpolateGap";
  private static final String TRANSACTION_METRICS_CONTEXT = "transactions";
  private static final String AUTO_RESOLUTION = "auto";

  public enum PathType {
    APPS,
    DATASETS,
    STREAMS,
    CLUSTER,
    SERVICES,
    SPARK
  }

  public enum ProgramType {
    FLOWS("f"),
    MAPREDUCE("b"),
    PROCEDURES("p"),
    HANDLERS("h"),
    SERVICES("u"),
    SPARK("s");

    private final String code;

    private ProgramType(String code) {
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

  enum Resolution {
    SECOND(1),
    MINUTE(60),
    HOUR(3600);

    private int resolution;
    private Resolution(int resolution) {
      this.resolution = resolution;
    }

    public int getResolution() {
      return resolution;
    }
  }

  public enum MetricsScope {
    SYSTEM,
    USER
  }

  private static String urlDecode(String str) {
    try {
      return URLDecoder.decode(str, CharEncoding.UTF_8);
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("unsupported encoding in path element", e);
    }
  }

  /**
   * Given a full metrics path like '/v2/metrics/system/apps/collect.events', strip the preceding version and
   * metrics to return 'system/apps/collect.events', representing the context and metric, which can then be
   * parsed by this parser.
   *
   * @param path request path.
   * @return request path stripped of version and metrics.
   */
  static String stripVersionAndMetricsFromPath(String path) {
    // +8 for "/metrics"
    int startPos = Constants.Gateway.API_VERSION_2.length() + 8;
    return path.substring(startPos, path.length());
  }

  static CubeDeleteQuery parseDelete(URI requestURI, String metricPrefix) throws MetricsPathException {
    CubeQueryBuilder builder = new CubeQueryBuilder();
    parseContext(requestURI.getPath(), builder);
    builder.setStartTs(0);
    builder.setEndTs(Integer.MAX_VALUE - 1);
    builder.setMetricName(metricPrefix);

    CubeQuery query = builder.build();
    return new CubeDeleteQuery(query.getStartTs(), query.getEndTs(), query.getMeasureName(), query.getMeasureType(),
                               query.getSliceByTags(), query.getMeasureName() != null);
  }
  static CubeQuery parse(URI requestURI) throws MetricsPathException {
    CubeQueryBuilder builder = new CubeQueryBuilder();
    // metric will be at the end.
    String uriPath = requestURI.getRawPath();
    int index = uriPath.lastIndexOf("/");
    builder.setMetricName(urlDecode(uriPath.substring(index + 1)));
    // strip the metric from the end of the path
    if (index != -1) {
      String strippedPath = uriPath.substring(0, index);
      if (strippedPath.startsWith("/system/cluster")) {
        builder.setSliceByTagValues(ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE,
                                                    Constants.Metrics.Tag.CLUSTER_METRICS, "true"));
        builder.setScope("system");
      } else if (strippedPath.startsWith("/system/transactions")) {
        builder.setSliceByTagValues(ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE,
                                                    Constants.Metrics.Tag.COMPONENT, TRANSACTION_METRICS_CONTEXT));
        builder.setScope("system");
      } else {
        parseContext(strippedPath, builder);
      }
    } else {
      builder.setSliceByTagValues(Maps.<String, String>newHashMap());
    }
    parseQueryString(requestURI, builder);

    return builder.build();
  }

  /**
   * Parse the context path, setting the relevant context fields in the builder.
   * Context starts after the scope and looks something like:
   * system/apps/{app-id}/{program-type}/{program-id}/{component-type}/{component-id}
   */
  static void parseContext(String path, CubeQueryBuilder builder) throws MetricsPathException {
    Map<String, String> tagValues = Maps.newHashMap();

    Iterator<String> pathParts = Splitter.on('/').omitEmptyStrings().split(path).iterator();

    // everything
    if (!pathParts.hasNext()) {
      builder.setSliceByTagValues(tagValues);
      return;
    }

    // scope is the first part of the path
    String scopeStr = pathParts.next();
    try {
      // we do conversion to validate value
      builder.setScope(MetricsScope.valueOf(scopeStr.toUpperCase()).toString().toLowerCase());
    } catch (IllegalArgumentException e) {
      throw new MetricsPathException("invalid scope: " + scopeStr);
    }


    // streams, datasets, apps, or nothing.
    if (!pathParts.hasNext()) {
      builder.setSliceByTagValues(tagValues);
      return;
    }

    // apps, streams, or datasets
    String pathTypeStr = pathParts.next();
    PathType pathType;
    try {
      pathType = PathType.valueOf(pathTypeStr.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new MetricsPathException("invalid type: " + pathTypeStr);
    }

    switch (pathType) {
      case APPS:
        // Note: If v3 APIs use this class, we may have to get namespaceId from higher up
        tagValues.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
        tagValues.put(Constants.Metrics.Tag.APP, urlDecode(pathParts.next()));
        parseSubContext(pathParts, tagValues);
        break;
      case STREAMS:
        // Note: If v3 APIs use this class, we may have to get namespaceId from higher up
        tagValues.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
        if (!pathParts.hasNext()) {
          throw new MetricsPathException("'streams' must be followed by a stream name");
        }
        tagValues.put(Constants.Metrics.Tag.STREAM, urlDecode(pathParts.next()));
        break;
      case DATASETS:
        // Note: If v3 APIs use this class, we may have to get namespaceId from higher up
        tagValues.put(Constants.Metrics.Tag.NAMESPACE, Constants.DEFAULT_NAMESPACE);
        if (!pathParts.hasNext()) {
          throw new MetricsPathException("'datasets' must be followed by a dataset name");
        }
        tagValues.put(Constants.Metrics.Tag.DATASET, urlDecode(pathParts.next()));
        // path can be /metric/scope/datasets/{dataset}/apps/...
        if (pathParts.hasNext()) {
          if (!pathParts.next().equals("apps")) {
            throw new MetricsPathException("expecting 'apps' after stream or dataset name");
          }
          tagValues.put(Constants.Metrics.Tag.APP, urlDecode(pathParts.next()));
          parseSubContext(pathParts, tagValues);
        }
        break;
      case SERVICES:
        // Note: If v3 APIs use this class, we may have to get namespaceId from higher up
        tagValues.put(Constants.Metrics.Tag.NAMESPACE, Constants.SYSTEM_NAMESPACE);
        parseSystemService(pathParts, tagValues);
        break;
    }

    if (pathParts.hasNext()) {
      throw new MetricsPathException("path contains too many elements: " + path);
    }

    builder.setSliceByTagValues(tagValues);
  }

  private static void parseSystemService(Iterator<String> pathParts, Map<String, String> tagValues)
    throws MetricsPathException {

    if (!pathParts.hasNext()) {
      throw new MetricsPathException("'services must be followed by a service name");
    }
    tagValues.put(Constants.Metrics.Tag.COMPONENT, urlDecode(pathParts.next()));
    if (!pathParts.hasNext()) {
      return;
    }
    // skipping "/handlers"
    String next = pathParts.next();
    if (!"handlers".equals(next)) {
      throw new MetricsPathException("'handlers must be followed by a service name");
    }
    tagValues.put(Constants.Metrics.Tag.HANDLER, urlDecode(pathParts.next()));
    if (!pathParts.hasNext()) {
      return;
    }
    // skipping "/runs"
    next = pathParts.next();
    if (RUN_ID.equals(next)) {
      tagValues.put(Constants.Metrics.Tag.RUN_ID, urlDecode(pathParts.next()));
      if (!pathParts.hasNext()) {
        return;
      }
      // skipping "/methods"
      pathParts.next();
    }

    tagValues.put(Constants.Metrics.Tag.METHOD, urlDecode(pathParts.next()));
  }

  /**
   * pathParts should look like {app-id}/{program-type}/{program-id}/{component-type}/{component-id}.
   */
  static void parseSubContext(Iterator<String> pathParts, Map<String, String> tagValues)
    throws MetricsPathException {

    // todo
//    if (!pathParts.hasNext()) {
//      return;
//    }
//    builder.setTypeId(urlDecode(pathParts.next()));

    if (!pathParts.hasNext()) {
      return;
    }

    // request-type: flows, procedures, or mapreduce or handlers or services(user)
    String pathProgramTypeStr = pathParts.next();
    ProgramType programType;
    try {
      programType = ProgramType.valueOf(pathProgramTypeStr.toUpperCase());
      tagValues.put(Constants.Metrics.Tag.PROGRAM_TYPE, programType.getCode());
    } catch (IllegalArgumentException e) {
      throw new MetricsPathException("invalid program type: " + pathProgramTypeStr);
    }

    // contextPrefix should look like appId.f right now, if we're looking at a flow
    if (!pathParts.hasNext()) {
      return;
    }
    tagValues.put(Constants.Metrics.Tag.PROGRAM, pathParts.next());

    if (!pathParts.hasNext()) {
      return;
    }

    switch (programType) {
      case MAPREDUCE:
        String mrTypeStr = pathParts.next();
        if (mrTypeStr.equals(RUN_ID)) {
          parseRunId(pathParts, tagValues);
          if (pathParts.hasNext()) {
            mrTypeStr = pathParts.next();
          } else {
            return;
          }
        }
        MapReduceType mrType;
        try {
          mrType = MapReduceType.valueOf(mrTypeStr.toUpperCase());
        } catch (IllegalArgumentException e) {
          throw new MetricsPathException("invalid mapreduce component: " + mrTypeStr
                                           + ".  must be 'mappers' or 'reducers'.");
        }
        tagValues.put(Constants.Metrics.Tag.MR_TASK_TYPE, mrType.getId());
        break;
      case FLOWS:
        buildFlowletContext(pathParts, tagValues);
        break;
      case HANDLERS:
        buildComponentTypeContext(pathParts, tagValues, "methods", "handler", Constants.Metrics.Tag.METHOD);
        break;
      case SERVICES:
        buildComponentTypeContext(pathParts, tagValues, "runnables", "service", Constants.Metrics.Tag.SERVICE_RUNNABLE);
        break;
      case PROCEDURES:
        if (pathParts.hasNext()) {
          if (pathParts.next().equals(RUN_ID)) {
            parseRunId(pathParts, tagValues);
          }
        }
        break;
      case SPARK:
        if (pathParts.hasNext()) {
          if (pathParts.next().equals(RUN_ID)) {
            parseRunId(pathParts, tagValues);
          }
        }
        break;
    }
    if (pathParts.hasNext()) {
      throw new MetricsPathException("path contains too many elements");
    }
  }

  private static void buildComponentTypeContext(Iterator<String> pathParts, Map<String, String> tagValues,
                                                String componentType, String requestType, String componentTagName)
    throws MetricsPathException {
    String nextPath = pathParts.next();

    if (nextPath.equals(RUN_ID)) {
      tagValues.put(Constants.Metrics.Tag.RUN_ID, pathParts.next());
      if (pathParts.hasNext()) {
        nextPath = pathParts.next();
      } else {
        return;
      }
    }
    if (!nextPath.equals(componentType)) {
      String exception = String.format("Expecting '%s' after the %s name ", componentType,
                                       requestType.substring(0, requestType.length() - 1));
      throw new MetricsPathException(exception);
    }
    if (!pathParts.hasNext()) {
      String exception = String.format("'%s' must be followed by a %s name ", componentType,
                                       componentType.substring(0, componentType.length() - 1));
      throw new MetricsPathException(exception);
    }
    tagValues.put(componentTagName, urlDecode(pathParts.next()));
  }

  private static void parseRunId(Iterator<String> pathParts, Map<String, String> tagValues)
    throws MetricsPathException {
    if (!pathParts.hasNext()) {
      throw new MetricsPathException("expecting " + RUN_ID + " value after the identifier runs in path");
    }
    tagValues.put(Constants.Metrics.Tag.RUN_ID, pathParts.next());
  }

  /**
   * At this point, pathParts should look like flowlets/{flowlet-id}/queues/{queue-id}, with queues being optional.
   */
  private static void buildFlowletContext(Iterator<String> pathParts, Map<String, String> tagValues)
    throws MetricsPathException {

    buildComponentTypeContext(pathParts, tagValues, "flowlets", "flows", Constants.Metrics.Tag.FLOWLET);
    if (pathParts.hasNext()) {
      if (!pathParts.next().equals("queues")) {
        throw new MetricsPathException("expecting 'queues' after the flowlet name");
      }
      if (!pathParts.hasNext()) {
        throw new MetricsPathException("'queues' must be followed by a queue name");
      }
      tagValues.put(Constants.Metrics.Tag.FLOWLET_QUEUE, urlDecode(pathParts.next()));
    }
  }

  /**
   * From the query string determine the query type, time range and related parameters.
   */
  public static void parseQueryString(URI requestURI, CubeQueryBuilder builder) throws MetricsPathException {
    Map<String, List<String>> queryParams = new QueryStringDecoder(requestURI).getParameters();
    parseTimeseries(queryParams, builder);
  }

  private static boolean isAutoResolution(Map<String, List<String>> queryParams) {
    return queryParams.get(RESOLUTION).get(0).equals(AUTO_RESOLUTION);
  }

  private static void parseTimeseries(Map<String, List<String>> queryParams, CubeQueryBuilder builder) {
    int count;
    long startTime;
    long endTime;
    int resolution;
    long now = TimeUnit.SECONDS.convert(System.currentTimeMillis(), TimeUnit.MILLISECONDS);

    if (queryParams.containsKey(RESOLUTION) && !isAutoResolution(queryParams)) {
      resolution = TimeMathParser.resolutionInSeconds(queryParams.get(RESOLUTION).get(0));
      if (!((resolution == 3600) || (resolution == 60) || (resolution == 1))) {
        throw new IllegalArgumentException("Resolution interval not supported, only 1 second, 1 minute and " +
                                             "1 hour resolutions are supported currently");
      }
    } else {
      // if resolution is not provided set default 1
      resolution = 1;
    }

    if (queryParams.containsKey(START_TIME) && queryParams.containsKey(END_TIME)) {
      startTime = TimeMathParser.parseTime(now, queryParams.get(START_TIME).get(0));
      endTime = TimeMathParser.parseTime(now, queryParams.get(END_TIME).get(0));
      if (queryParams.containsKey(RESOLUTION)) {
        if (isAutoResolution(queryParams)) {
          // auto determine resolution, based on time difference.
          Resolution autoResolution = getResolution(endTime - startTime);
          resolution = autoResolution.getResolution();
        }
      } else {
        resolution = Resolution.SECOND.getResolution();
      }
      count = (int) (((endTime / resolution * resolution) - (startTime / resolution * resolution)) / resolution + 1);
    } else if (queryParams.containsKey(COUNT)) {
      count = Integer.parseInt(queryParams.get(COUNT).get(0));
      // both start and end times are inclusive, which is the reason for the +-1.
      if (queryParams.containsKey(START_TIME)) {
        startTime = TimeMathParser.parseTime(now, queryParams.get(START_TIME).get(0));
        endTime = startTime + (count * resolution) - resolution;
      } else if (queryParams.containsKey(END_TIME)) {
        endTime = TimeMathParser.parseTime(now, queryParams.get(END_TIME).get(0));
        startTime = endTime - (count * resolution) + resolution;
      } else {
        // if only count is specified, assume the current time is desired as the end.
        endTime = now - MetricsConstants.QUERY_SECOND_DELAY;
        startTime = endTime - (count * resolution) + resolution;
      }
    } else {
      startTime = 0;
      endTime = 0;
      count = 1;
      // max int means aggregate "all time totals"
      resolution = Integer.MAX_VALUE;
    }

    builder.setStartTs(startTime);
    builder.setEndTs(endTime);
    builder.setLimit(count);
    builder.setResolution(resolution);

    setInterpolator(queryParams, builder);
  }

  private static Resolution getResolution(long difference) {
    if (difference >= MetricsConstants.METRICS_HOUR_RESOLUTION_CUTOFF) {
      return  Resolution.HOUR;
    } else if (difference >= MetricsConstants.METRICS_MINUTE_RESOLUTION_CUTOFF) {
      return Resolution.MINUTE;
    } else {
      return Resolution.SECOND;
    }
  }

  private static void setInterpolator(Map<String, List<String>> queryParams, CubeQueryBuilder builder) {
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
    // todo: support interpolator in CubeQuery
//    builder.setInterpolator(interpolator);
  }

  static class CubeQueryBuilder {
    private long startTs;
    private long endTs;
    private int resolution;
    private String scope;
    private String metricName;
    // todo: should be aggregation? e.g. also support min/max, etc.
    private Map<String, String> sliceByTagValues;
    private int limit;

    public void setStartTs(long startTs) {
      this.startTs = startTs;
    }

    public void setEndTs(long endTs) {
      this.endTs = endTs;
    }

    public void setResolution(int resolution) {
      this.resolution = resolution;
    }

    public void setMetricName(String metricName) {
      this.metricName = metricName;
    }

    public void setScope(String scope) {
      this.scope = scope;
    }

    public void setSliceByTagValues(Map<String, String> sliceByTagValues) {
      this.sliceByTagValues = sliceByTagValues;
    }

    public void setLimit(int limit) {
      this.limit = limit;
    }

    public CubeQuery build() {
      String measureName = (metricName != null && scope != null) ? scope + "." + metricName : null;
      return new CubeQuery(startTs, endTs, resolution, limit, measureName, MeasureType.COUNTER,
                           sliceByTagValues, new ArrayList<String>());
    }
  }
}
