package com.payvment.continuuity;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.Procedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.payvment.continuuity.data.ActivityFeed;
import com.payvment.continuuity.data.ActivityFeedTable;
import com.payvment.continuuity.data.ClusterFeedReader;
import com.payvment.continuuity.data.ClusterTable;
import com.payvment.continuuity.data.PopularFeed;
import com.payvment.continuuity.data.PopularFeed.PopularFeedEntry;
import com.payvment.continuuity.data.SortedCounterTable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Exposes Payvment Lish feeds as Queries (and then REST calls in the Gateway).
 * <p>
 * Implemented as a {@link Procedure} to bridge between external calls and
 * the internal APIs of {@link ClusterFeedReader}.
 * <p>
 * Example queries:
 * <pre>
 *   http://localhost:10003/rest-query/feedreader/readactivity?clusterid=3&limit=10&country=US
 *   <p>
 *   http://localhost:10003/rest-query/feedreader/readpopular?clusterid=3&numhours=24&limit=10&country=US
 * </pre>
 */
public class ClusterFeedQueryProvider extends AbstractProcedure {

  private ClusterFeedReader reader = null;

  @UseDataSet(LishApp.CLUSTER_TABLE)
  private ClusterTable clusterTable;

  @UseDataSet(LishApp.TOP_SCORE_TABLE)
  private SortedCounterTable topScoreTable;

  @UseDataSet(LishApp.ACTIVITY_FEED_TABLE)
  private ActivityFeedTable activityFeedTable;
  
  private void initReader() {
    if (this.reader == null) {
      this.reader = new ClusterFeedReader(clusterTable, topScoreTable,
          activityFeedTable);
    }
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
   * @param request method being performed
   * @param responder arguments of method
   * @return string result
   */
  @Handle("readactivity")
  public void readActivity(ProcedureRequest request, ProcedureResponder responder) throws Exception {

    initReader();


    String country =  request.getArgument("country"); // args.get("country");
    String clusteridStr = request.getArgument("clusterid"); // args.get("clusterid");
    String limitStr =  request.getArgument("limit");  //  args.get("limit");
    Integer clusterid = null;
    Integer limit = null;
    Long maxts = Long.MAX_VALUE;
    Long mints = 0L;

    try {
      clusterid = Integer.valueOf(clusteridStr);
      limit = Integer.valueOf(limitStr);

      if (request.getArgument("maxts") != null)
        maxts = Long.getLong(request.getArgument("maxts"));

      if (request.getArgument("mints") != null)
        mints = Long.getLong(request.getArgument("mints"));

    } catch (NumberFormatException nfe) {
      throw nfe;
    }

    try {
      ActivityFeed feed =
          this.reader.getActivityFeed(country, clusterid, limit, maxts, mints);

      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), feed);

    } catch (OperationException e) {
      throw e;
    }
  }


  @Handle("readpopular")
  public void readPopular(ProcedureRequest request, ProcedureResponder responder) throws Exception {

    initReader();

    String country =  request.getArgument("country");//  args.get("country");
    String clusteridStr = request.getArgument("clusterid"); // args.get("clusterid");
    String numhoursStr = request.getArgument("numhours") ; // args.get("numhours");
    String limitStr = request.getArgument("limit");    //   args.get("limit");
    Integer clusterid = null;
    Integer numhours = null;
    Integer limit = null;

    // Check for additional arguments (first setting default values)
    Integer offset = 0;
    Long starttime = System.currentTimeMillis();


    try {
      clusterid = Integer.valueOf(clusteridStr);
      numhours = Integer.valueOf(numhoursStr);
      limit = Integer.valueOf(limitStr);

      if (request.getArgument("offset") != null)
        offset = Integer.valueOf(request.getArgument("offset"));

      if (request.getArgument("starttime") != null)
        starttime = Long.valueOf(request.getArgument("starttime"));

    } catch (NumberFormatException nfe) {
      throw nfe;
    }

    PopularFeed feed = this.reader.getPopularFeed(country, clusterid,
        TimeUnit.MILLISECONDS.toHours(starttime),
        numhours, limit, offset);
    List<PopularFeedEntry> entries = feed.getFeed(limit + offset);

    responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), entries);
  }

  /// Old logic
  //  @Override
  //  public QueryProviderResponse process(String methodName, Map<String, String> args) {
  //    if (this.reader == null) {
  //      this.reader = new ClusterFeedReader(this.clusterTable, this.topScoreTable,
  //          this.activityFeedTable);
  //    }
  //    if (args == null || args.isEmpty()) {
  //      getQueryProviderContext().getLogger().warn(
  //          "Received request for method '" + methodName + "' but " +
  //          "contained no arguments (args=" + args + ")");
  //    }
  //    if (TRACE) {
  //      String str = "Received method " + methodName + " with args " +
  //          toString(args);
  //      getQueryProviderContext().getLogger().trace(str);
  //    }
  //
  //    // Determine if the methodName type and args are valid
  //    // If they are valid, call specific method to perform query
  //    // Currently only supported methods are 'readactivity' and 'readpopular'
  //
  //    if (methodName.equals("readactivity")) {
  //
  //      // Requires clusterid and limit
  //      if (!args.containsKey("clusterid") ||
  //          !args.containsKey("limit") ||
  //          !args.containsKey("country")) {
  //        String msg = "Received 'readactivity' query but without a required " +
  //            "argument (args=" + toString(args) + ")";
  //        getQueryProviderContext().getLogger().error(msg);
  //        return new QueryProviderResponse(Status.FAILED, msg, msg);
  //      }
  //      String country = args.get("country");
  //      String clusteridStr = args.get("clusterid");
  //      String limitStr = args.get("limit");
  //      Integer clusterid = null;
  //      Integer limit = null;
  //      try {
  //        clusterid = Integer.valueOf(clusteridStr);
  //        limit = Integer.valueOf(limitStr);
  //      } catch (NumberFormatException nfe) {
  //        String msg ="Numeric argument was not in an acceptable format " +
  //          "(args=" + toString(args) + ")";
  //        getQueryProviderContext().getLogger().error(msg, nfe);
  //        return new QueryProviderResponse(Status.FAILED, msg, msg);
  //      }
  //
  //      // Check for additional arguments (first setting default values)
  //      Long maxts = Long.MAX_VALUE;
  //      Long mints = 0L;
  //      try {
  //        if (args.containsKey("maxts")) {
  //          maxts = Long.valueOf(args.get("maxts"));
  //        }
  //        if (args.containsKey("mints")) {
  //          mints = Long.valueOf(args.get("mints"));
  //        }
  //      } catch (NumberFormatException nfe) {
  //        String msg = "Numeric argument was not in an acceptable format " +
  //            "(args=" + toString(args) + ")";
  //        getQueryProviderContext().getLogger().error(msg, nfe);
  //        return new QueryProviderResponse(Status.FAILED, msg, msg);
  //      }
  //
  //      // All arguments parsed and verified.  Call activity feed read method.
  //      return executeActivityFeedRead(country, clusterid, limit, maxts, mints);
  //
  //    } else if (methodName.equals("readpopular")) {
  //
  //      // Requires clusterid, numhours, and limit
  //      if (!args.containsKey("clusterid") ||
  //          !args.containsKey("numhours") ||
  //          !args.containsKey("limit") ||
  //          !args.containsKey("country")) {
  //        String msg = "Received 'readpopular' query but without a required " +
  //          "argument (args=" + toString(args) + ")";
  //        getQueryProviderContext().getLogger().error(msg);
  //        return new QueryProviderResponse(Status.FAILED, msg, msg);
  //      }
  //      String country = args.get("country");
  //      String clusteridStr = args.get("clusterid");
  //      String numhoursStr = args.get("numhours");
  //      String limitStr = args.get("limit");
  //      Integer clusterid = null;
  //      Integer numhours = null;
  //      Integer limit = null;
  //      try {
  //        clusterid = Integer.valueOf(clusteridStr);
  //        numhours = Integer.valueOf(numhoursStr);
  //        limit = Integer.valueOf(limitStr);
  //      } catch (NumberFormatException nfe) {
  //        String msg = "Numeric argument was not in an acceptable format " +
  //          "(args=" + toString(args) + ")";
  //        getQueryProviderContext().getLogger().error(msg, nfe);
  //        return new QueryProviderResponse(Status.FAILED, msg, msg);
  //      }
  //
  //      // Check for additional arguments (first setting default values)
  //      Integer offset = 0;
  //      Long starttime = System.currentTimeMillis();
  //      try {
  //        if (args.containsKey("offset")) {
  //          offset = Integer.valueOf(args.get("offset"));
  //        }
  //        if (args.containsKey("starttime")) {
  //          starttime = Long.valueOf(args.get("starttime"));
  //        }
  //      } catch (NumberFormatException nfe) {
  //        String msg = "Numeric argument was not in an acceptable format " +
  //            "(args=" + toString(args) + ")";
  //        getQueryProviderContext().getLogger().error(msg, nfe);
  //        return new QueryProviderResponse(Status.FAILED, msg, msg);
  //      }
  //
  //      // All arguments parsed and verified.  Call popular feed read method.
  //      return executePopularFeedRead(country, clusterid, starttime, numhours,
  //          limit, offset);
  //
  //    } else {
  //
  //      // Invalid method
  //      String msg = "Invalid read method.  method=" + methodName + ", args=" +
  //          toString(args);
  //      getQueryProviderContext().getLogger().error(msg);
  //      return new QueryProviderResponse(Status.FAILED, msg, msg);
  //    }
  //  }



  //  private QueryProviderResponse executeActivityFeedRead(String country,
  //      Integer clusterid, Integer limit, Long maxts, Long mints) {
  //    try {
  //      ActivityFeed feed =
  //          reader.getActivityFeed(country, clusterid, limit, maxts, mints);
  //      return new QueryProviderResponse(ActivityFeed.toJson(feed));
  //    } catch (OperationException e) {
  //      String msg = "Exception reading activity feed (clusterid= " + clusterid +
  //          ", limit=" + limit + ", maxts=" + maxts + ", mints=" + mints + ")";
  //      getQueryProviderContext().getLogger().error(msg, e);
  //      return new QueryProviderResponse(Status.FAILED, msg, msg);
  //    }
  //  }

  //  private QueryProviderResponse executePopularFeedRead(String country,
  //      Integer clusterid, Long starttime, Integer numhours, Integer limit,
  //      Integer offset) {
  //    try {
  //      PopularFeed feed = reader.getPopularFeed(country, clusterid,
  //          Helpers.hour(starttime), numhours, limit, offset);
  //      List<PopularFeedEntry> entries = feed.getFeed(limit + offset);
  //      String jsonResult = null;
  //      if (offset == 0) {
  //        jsonResult = PopularFeed.toJson(entries);
  //      } else if (offset >= entries.size()) {
  //        entries.clear();
  //        jsonResult = PopularFeed.toJson(entries);
  //      } else {
  //        entries = entries.subList(offset, entries.size());
  //        jsonResult = PopularFeed.toJson(entries);
  //      }
  //      return new QueryProviderResponse(jsonResult);
  //    } catch (OperationException e) {
  //      String msg = "Exception reading popular feed (clusterid= " + clusterid +
  //          ", limit=" + limit + ", numhours=" + numhours + ", offset=" + offset;
  //      getQueryProviderContext().getLogger().error(msg, e);
  //      return new QueryProviderResponse(Status.FAILED, msg, msg);
  //    }
  //  }

  @SuppressWarnings("unused")
  private String toString(Map<String, String> args) {
    String str = "(";
    for (Map.Entry<String, String> arg : args.entrySet()) {
      str += arg.getKey() + "=" + arg.getValue() + " ";
    }
    str += ")";
    return str.substring(0, str.length() - 1);
  }
}
