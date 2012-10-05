package com.payvment.continuuity.data;

import java.util.Map;
import java.util.TreeMap;

import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.continuuity.api.data.DataFabric;
import com.continuuity.api.data.QueryRestProvider;

public class FeedQueryRestProvider extends QueryRestProvider {

  private static final Logger LOG = LoggerFactory
      .getLogger(FeedQueryRestProvider.class);

  private final DataFabric fabric;

  private final ClusterFeedReader reader;
  
  public FeedQueryRestProvider(DataFabric fabric) {
    this.fabric = fabric;
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
   * This currently supports two 
   * @param message
   * @param readMethod
   * @param args
   */
  private void executeQuery(MessageEvent message, String readMethod,
      Map<String, String> args) {
    HttpRequest request = (HttpRequest)message.getMessage();
    String str = "Received method " + readMethod + " with args ";
    for (Map.Entry<String, String> arg : args.entrySet()) {
      str += arg.getKey() + "=" + arg.getValue() + " ";
    }
    LOG.info(str);
    
    // 
    super.respondSuccess(message.getChannel(), request);
  }

}
