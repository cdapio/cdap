package com.payvment.continuuity;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.lib.SortedCounterTable;
import com.continuuity.api.data.util.Helpers;
import com.continuuity.api.query.QueryProvider;
import com.continuuity.api.query.QueryProviderContentType;
import com.continuuity.api.query.QueryProviderResponse;
import com.continuuity.api.query.QueryProviderResponse.Status;
import com.continuuity.api.query.QuerySpecifier;
import com.payvment.continuuity.data.ActivityFeed;
import com.payvment.continuuity.data.ClusterFeedReader;
import com.payvment.continuuity.data.ClusterTable;
import com.payvment.continuuity.data.PopularFeed;
import com.payvment.continuuity.data.PopularFeed.PopularFeedEntry;

/**
 * Exposes Payvment Lish feeds as Queries (and then REST calls in the Gateway).
 * <p>
 * Implemented as a {@link QueryProvider} to bridge between external calls and
 * the internal APIs of {@link ClusterFeedReader}.
 * <p>
 * Example queries:
 * <pre>
 *   http://localhost:10003/rest-query/feedreader/readactivity?clusterid=3&limit=10
 *   <p>
 *   http://localhost:10003/rest-query/feedreader/readpopular?clusterid=3&numhours=24&limit=10
 * </pre>
 */
public class ClusterFeedQueryProvider extends QueryProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(ClusterFeedQueryProvider.class);

  private ClusterFeedReader reader = null;

  @Override
  public void configure(QuerySpecifier specifier) {
    specifier.service("feedreader");
    specifier.timeout(20000);
    specifier.type(QueryProviderContentType.JSON);
    specifier.provider(ClusterFeedQueryProvider.class);
  }

  private ClusterTable clusterTable;

  private SortedCounterTable topScoreTable;
  
  @Override
  public void initialize() {
    this.clusterTable = new ClusterTable();
    getQueryProviderContext().getDataSetRegistry().registerDataSet(
        this.clusterTable);
    this.topScoreTable = new SortedCounterTable("topScores",
          new SortedCounterTable.SortedCounterConfig());
    getQueryProviderContext().getDataSetRegistry().registerDataSet(
        this.topScoreTable);
  }

  /**
   * Performs the query once the URL has been validated and the inputs have been
   * parsed.  Further validation of arguments required per-method is performed
   * within this method.
   * <p>
   * This currently supports two methods, <i>readactivity</i> and
   * <i>readpopular</i>.
   * <p>
   * <b><i>readactivity</i></b> performs an ActivityFeed read and has required
   * arguments of <i>clusterid</i> and <i>limit</i>.  Optionally, can also
   * specify a <i>maxts</i> and <i>mints</i> for maximum and minimum timestamps.
   * <p>
   * <b><i>readpopular</i></b> performs a PopularFeed read and has required
   * arguments of <i>clusterid</i>, <i>numhours</i>, and <i>limit</i>.
   * Optionally, can also specify an <i>offset</i> and <i>starttime</i>, which
   * is treated as the start time for doing popular queries.
   * @param methodName method being performed
   * @param args arguments of method
   * @return string result
   */
  @Override
  public QueryProviderResponse process(String methodName,
      Map<String, String> args) {
    if (this.reader == null) {
      this.reader = new ClusterFeedReader(this.clusterTable, this.topScoreTable,
          getQueryProviderContext().getDataFabric());
    }
    if (args == null || args.isEmpty()) {
      LOG.warn("Received request for method '" + methodName + "' but " +
          "contained no arguments (args=" + args + ")");
    }
    String str = "Received method " + methodName + " with args " +
        toString(args);
    LOG.info(str);
    
    // Determine if the methodName type and args are valid
    // If they are valid, call specific method to perform query
    // Currently only supported methods are 'readactivity' and 'readpopular'
    
    if (methodName.equals("readactivity")) {
      
      // Requires clusterid and limit
      if (!args.containsKey("clusterid") ||
          !args.containsKey("limit")) {
        String msg = "Received 'readactivity' query but without a required " +
            "argument (args=" + toString(args) + ")";
        LOG.warn(msg);
        return new QueryProviderResponse(Status.FAILED, msg, msg);
      }
      String clusteridStr = args.get("clusterid");
      String limitStr = args.get("limit");
      Integer clusterid = null;
      Integer limit = null;
      try {
        clusterid = Integer.valueOf(clusteridStr);
        limit = Integer.valueOf(limitStr);
      } catch (NumberFormatException nfe) {
        String msg ="Numeric argument was not in an acceptable format " +
          "(args=" + toString(args) + ")";
        LOG.warn(msg, nfe);
        return new QueryProviderResponse(Status.FAILED, msg, msg);
      }
      
      // Check for additional arguments (first setting default values)
      Long maxts = Long.MAX_VALUE;
      Long mints = 0L;
      try {
        if (args.containsKey("maxts")) {
          maxts = Long.valueOf(args.get("maxts"));
        }
        if (args.containsKey("mints")) {
          mints = Long.valueOf(args.get("mints"));
        }
      } catch (NumberFormatException nfe) {
        String msg = "Numeric argument was not in an acceptable format " +
            "(args=" + toString(args) + ")";
        LOG.warn(msg, nfe);
        return new QueryProviderResponse(Status.FAILED, msg, msg);
      }
      
      // All arguments parsed and verified.  Call activity feed read method.
      return executeActivityFeedRead(clusterid, limit, maxts, mints);
      
    } else if (methodName.equals("readpopular")) {

      // Requires clusterid, numhours, and limit
      if (!args.containsKey("clusterid") ||
          !args.containsKey("numhours") ||
          !args.containsKey("limit")) {
        String msg = "Received 'readpopular' query but without a required " +
          "argument (args=" + toString(args) + ")";
        LOG.warn(msg);
        return new QueryProviderResponse(Status.FAILED, msg, msg);
      }
      String clusteridStr = args.get("clusterid");
      String numhoursStr = args.get("numhours");
      String limitStr = args.get("limit");
      Integer clusterid = null;
      Integer numhours = null;
      Integer limit = null;
      try {
        clusterid = Integer.valueOf(clusteridStr);
        numhours = Integer.valueOf(numhoursStr);
        limit = Integer.valueOf(limitStr);
      } catch (NumberFormatException nfe) {
        String msg = "Numeric argument was not in an acceptable format " +
          "(args=" + toString(args) + ")";
        LOG.warn(msg, nfe);
        return new QueryProviderResponse(Status.FAILED, msg, msg);
      }
      
      // Check for additional arguments (first setting default values)
      Integer offset = 0;
      Long starttime = System.currentTimeMillis();
      try {
        if (args.containsKey("offset")) {
          offset = Integer.valueOf(args.get("offset"));
        }
        if (args.containsKey("starttime")) {
          starttime = Long.valueOf(args.get("starttime"));
        }
      } catch (NumberFormatException nfe) {
        String msg = "Numeric argument was not in an acceptable format " +
            "(args=" + toString(args) + ")";
        LOG.warn(msg, nfe);
        return new QueryProviderResponse(Status.FAILED, msg, msg);
      }
      
      // All arguments parsed and verified.  Call popular feed read method.
      return executePopularFeedRead(clusterid, starttime, numhours, limit,
          offset);
      
    } else {
      
      // Invalid method
      String msg = "Invalid read method.  method=" + methodName + ", args=" +
          toString(args);
      LOG.error(msg);
      return new QueryProviderResponse(Status.FAILED, msg, msg);
    }
  }

  private QueryProviderResponse executeActivityFeedRead(Integer clusterid,
      Integer limit, Long maxts, Long mints) {
    try {
      ActivityFeed feed =
          reader.getActivityFeed(clusterid, limit, maxts, mints);
      return new QueryProviderResponse(ActivityFeed.toJson(feed));
    } catch (OperationException e) {
      String msg = "Exception reading activity feed (clusterid= " + clusterid +
          ", limit=" + limit + ", maxts=" + maxts + ", mints=" + mints + ")";
      LOG.warn(msg, e);
      return new QueryProviderResponse(Status.FAILED, msg, msg);
    }
  }

  private QueryProviderResponse executePopularFeedRead(Integer clusterid,
      Long starttime, Integer numhours, Integer limit, Integer offset) {
    try {
      PopularFeed feed = reader.getPopularFeed(clusterid,
          Helpers.hour(starttime), numhours, limit, offset);
      List<PopularFeedEntry> entries = feed.getFeed(limit + offset);
      String jsonResult = null;
      if (offset == 0) {
        jsonResult = PopularFeed.toJson(entries);
      } else if (offset >= entries.size()) {
        entries.clear();
        jsonResult = PopularFeed.toJson(entries);
      } else {
        entries = entries.subList(offset, entries.size());
        jsonResult = PopularFeed.toJson(entries);
      }
      return new QueryProviderResponse(jsonResult);
    } catch (OperationException e) {
      String msg = "Exception reading popular feed (clusterid= " + clusterid +
          ", limit=" + limit + ", numhours=" + numhours + ", offset=" + offset;
      LOG.warn(msg, e);
      return new QueryProviderResponse(Status.FAILED, msg, msg);
    }
  }

  private String toString(Map<String, String> args) {
    String str = "(";
    for (Map.Entry<String, String> arg : args.entrySet()) {
      str += arg.getKey() + "=" + arg.getValue() + " ";
    }
    str += ")";
    return str.substring(0, str.length() - 1);
  }
}
