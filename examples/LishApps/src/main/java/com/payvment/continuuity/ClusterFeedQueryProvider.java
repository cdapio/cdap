/*
 * Copyright (c) 2013, Continuuity Inc
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms,
 * with or without modification, are not permitted
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE
 * GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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
 * <p/>
 * Implemented as a {@link Procedure} to bridge between external calls and
 * the internal APIs of {@link ClusterFeedReader}.
 * <p/>
 * Example queries:
 * <pre>
 *   http://localhost:10003/rest-query/feedreader/readactivity?clusterid=3&limit=10&country=US
 *
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
      this.reader = new ClusterFeedReader(clusterTable, topScoreTable, activityFeedTable);
    }
  }

  /**
   * Performs the query once the URL has been validated and the inputs have been
   * parsed.  Further validation of arguments required per-method is performed
   * within this method.
   * <p/>
   * This currently supports two methods, <i>readactivity</i> and
   * <i>readpopular</i>.
   * <p/>
   * <b><i>readactivity</i></b> performs an ActivityFeed read and has required
   * arguments of <i>clusterid</i> and <i>limit</i>.  Optionally, can also
   * specify a <i>maxts</i> and <i>mints</i> for maximum and minimum timestamps.
   * <p/>
   * <b><i>readpopular</i></b> performs a PopularFeed read and has required
   * arguments of <i>clusterid</i>, <i>numhours</i>, and <i>limit</i>.
   * Optionally, can also specify an <i>offset</i> and <i>starttime</i>, which
   * is treated as the start time for doing popular queries.
   *
   * @param request   method being performed
   * @param responder arguments of method
   */
  @Handle("readactivity")
  public void readActivity(ProcedureRequest request, ProcedureResponder responder) throws Exception {

    initReader();


    String country = request.getArgument("country"); // args.get("country");
    String clusteridStr = request.getArgument("clusterid"); // args.get("clusterid");
    String limitStr = request.getArgument("limit");  //  args.get("limit");
    Integer clusterid = null;
    Integer limit = null;
    Long maxts = Long.MAX_VALUE;
    Long mints = 0L;

    try {
      clusterid = Integer.valueOf(clusteridStr);
      limit = Integer.valueOf(limitStr);

      if (request.getArgument("maxts") != null) {
        maxts = Long.getLong(request.getArgument("maxts"));
      }

      if (request.getArgument("mints") != null) {
        mints = Long.getLong(request.getArgument("mints"));
      }

    } catch (NumberFormatException nfe) {
      throw nfe;
    }

    try {
      ActivityFeed feed = this.reader.getActivityFeed(country, clusterid, limit, maxts, mints);

      responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), feed);

    } catch (OperationException e) {
      throw e;
    }
  }


  @Handle("readpopular")
  public void readPopular(ProcedureRequest request, ProcedureResponder responder) throws Exception {

    initReader();

    String country = request.getArgument("country");
    String clusteridStr = request.getArgument("clusterid");
    String numhoursStr = request.getArgument("numhours");
    String limitStr = request.getArgument("limit");
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

      if (request.getArgument("offset") != null) {
        offset = Integer.valueOf(request.getArgument("offset"));
      }

      if (request.getArgument("starttime") != null) {
        starttime = Long.valueOf(request.getArgument("starttime"));
      }

    } catch (NumberFormatException nfe) {
      throw nfe;
    }

    PopularFeed feed = this.reader.getPopularFeed(country,
                                                  clusterid,
                                                  TimeUnit.MILLISECONDS.toHours(starttime),
                                                  numhours,
                                                  limit,
                                                  offset);

    List<PopularFeedEntry> entries = feed.getFeed(limit + offset);

    responder.sendJson(new ProcedureResponse(ProcedureResponse.Code.SUCCESS), entries);
  }


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
