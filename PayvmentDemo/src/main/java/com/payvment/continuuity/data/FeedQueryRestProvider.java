package com.payvment.continuuity.data;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.QueryRestProvider;
import com.payvment.continuuity.data.PopularFeed.PopularFeedEntry;
import com.payvment.continuuity.util.Bytes;

/**
 * Exposes Payvment Lish feeds as REST calls in the Gateway.
 * <p>
 * Implemented as a {@link QueryRestProvider} to bridge between REST calls and
 * the internal APIs of {@link ClusterFeedReader}.
 */
public class FeedQueryRestProvider extends QueryRestProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(FeedQueryRestProvider.class);

  private final ClusterFeedReader reader;
  
  public FeedQueryRestProvider(DataFabric fabric) {
    this.reader = new ClusterFeedReader(fabric);
  }
  
  @Override
  public void executeQuery(MessageEvent message) {
    HttpRequest request = (HttpRequest)message.getMessage();
    HttpMethod method = request.getMethod();
    String requestUri = request.getUri();
    
    LOG.info("Received request " + method + " on  URL " + requestUri + "");
    
    // Perform pre-processing of the URI to determine values for the following
    // fields
    String readMethod = null;
    Map<String,String> args = new TreeMap<String,String>();
    
    // Split string by '/' and check that the URI is valid
    // If it is valid, set readMethod and args, otherwise return a bad request
    
    String [] pathSections = requestUri.split("/");
    
    if (pathSections.length != 4) {
      LOG.error("Expected to split URI into four chunks but found " +
          pathSections.length);
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      return;
    }
    
    if (!pathSections[2].equals("feedreader")) {
      LOG.error("Only query set supported is 'feedreader' but received a " +
          "request for " + pathSections[1]);
      super.respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      return;
    }
    
    // String split is okay, now parse up the method and args and store them
    
    String [] querySections = pathSections[3].split("\\?");

    if (querySections.length != 2) {
      LOG.error("Expected to split method+args into two chunks but found " +
          querySections.length);
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
      return;
    }
    
    readMethod = querySections[0];
    
    String [] argSections = querySections[1].split("&");
    for (String arg : argSections) {
      String [] argSplit = arg.split("=");
      if (argSplit.length != 2) {
        LOG.error("Expected to split an arg into two chunks but found " +
            argSplit.length);
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        return;
      }
      args.put(argSplit[0], argSplit[1]);
    }
    
    // Done with pre-processing, now have readMethod and args map
    executeQuery(message, readMethod, args);
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
   * arguments of <i>clusterid</i> and <i>limit</i>.  Optionally, can also specify
   * a <i>maxts</i> and <i>mints</i> for maximum and minimum timestamps.
   * <p>
   * <b><i>readpopular</i></b> performs a PopularFeed read and has required
   * arguments of <i>clusterid</i>, <i>numhours</i>, and <i>limit</i>.
   * Optionally, can also specify an <i>offset</i>. 
   * @param message
   * @param readMethod
   * @param args
   */
  private void executeQuery(MessageEvent message, String readMethod,
      Map<String, String> args) {
    String str = "Received method " + readMethod + " with args " +
        toString(args);
    LOG.info(str);
    System.out.println(str);
    
    // Determine if the readMethod type and args are valid
    // If they are valid, call specific method to perform query
    // Currently only supported methods are 'readactivity' and 'readpopular'
    
    if (readMethod.equals("readactivity")) {
      
      // Requires clusterid and limit
      if (!args.containsKey("clusterid") ||
          !args.containsKey("limit")) {
        LOG.warn("Received 'readactivity' query but without a required " +
          "argument (args=" + toString(args) + ")");
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        return;
      }
      String clusteridStr = args.get("clusterid");
      String limitStr = args.get("limit");
      Integer clusterid = null;
      Integer limit = null;
      try {
        clusterid = Integer.valueOf(clusteridStr);
        limit = Integer.valueOf(limitStr);
      } catch (NumberFormatException nfe) {
        LOG.warn("Numeric argument was not in an acceptable format " +
          "(args=" + toString(args) + ")", nfe);
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        return;
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
        LOG.warn("Numeric argument was not in an acceptable format " +
            "(args=" + toString(args) + ")", nfe);
          respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
          return;
      }
      
      // All arguments parsed and verified.  Call activity feed read method.
      executeActivityFeedRead(message, clusterid, limit, maxts, mints);
      
    } else if (readMethod.equals("readpopular")) {

      // Requires clusterid, numhours, and limit
      if (!args.containsKey("clusterid") ||
          !args.containsKey("numhours") ||
          !args.containsKey("limit")) {
        LOG.warn("Received 'readpopular' query but without a required " +
          "argument (args=" + toString(args) + ")");
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        return;
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
        LOG.warn("Numeric argument was not in an acceptable format " +
          "(args=" + toString(args) + ")", nfe);
        respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
        return;
      }
      
      // Check for additional arguments (first setting default values)
      Integer offset = 0;
      try {
        if (args.containsKey("offset")) {
          offset = Integer.valueOf(args.get("offset"));
        }
      } catch (NumberFormatException nfe) {
        LOG.warn("Numeric argument was not in an acceptable format " +
            "(args=" + toString(args) + ")", nfe);
          respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
          return;
      }
      
      // All arguments parsed and verified.  Call popular feed read method.
      executePopularFeedRead(message, clusterid, numhours, limit, offset);
      
    } else {
      
      // Invalid method
      LOG.warn("Invalid read method.  method=" + readMethod + ", args=" +
          toString(args));
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
    }
  }

  private void executeActivityFeedRead(MessageEvent message, Integer clusterid,
      Integer limit, Long maxts, Long mints) {
    try {
      ActivityFeed feed =
          reader.getActivityFeed(clusterid, limit, maxts, mints);
      respondSuccess(message.getChannel(), (HttpRequest)message.getMessage(),
          Bytes.toBytes(ActivityFeed.toJson(feed)));
    } catch (OperationException e) {
      LOG.warn("Exception reading activity feed (clusterid= " + clusterid +
          ", limit=" + limit + ", maxts=" + maxts + ", mints=" + mints + ")",
          e);
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
    }
  }

  private void executePopularFeedRead(MessageEvent message, Integer clusterid,
      Integer numhours, Integer limit, Integer offset) {
    try {
      PopularFeed feed =
          reader.getPopularFeed(clusterid, numhours, limit, offset);
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
      respondSuccess(message.getChannel(), (HttpRequest)message.getMessage(),
          Bytes.toBytes(jsonResult));
    } catch (OperationException e) {
      LOG.warn("Exception reading popular feed (clusterid= " + clusterid +
          ", limit=" + limit + ", numhours=" + numhours + ", offset=" + offset,
          e);
      respondError(message.getChannel(), HttpResponseStatus.BAD_REQUEST);
    }
  }

  private String toString(Map<String, String> args) {
    String str = "";
    for (Map.Entry<String, String> arg : args.entrySet()) {
      str += arg.getKey() + "=" + arg.getValue() + " ";
    }
    return str.substring(0, str.length() - 1);
  }
}
