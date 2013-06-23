//package com.payvment.continuuity.tools;
//
//import java.io.IOException;
//
//import org.apache.http.HttpResponse;
//import org.apache.http.HttpStatus;
//import org.apache.http.client.ClientProtocolException;
//import org.apache.http.client.HttpClient;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.impl.client.DefaultHttpClient;
//
///**
// * Command-line program for performing feed reads via REST and the Gateway.
// * <p>
// * Usage:
// * <pre>
// *   ./feed-reader hostname:port &lt;feed_type> [type_specific_args]
// * </pre>
// * Where available types are currently <i>activity</i> and <i>popular</i>.
// * <p>
// * Activity Usage (requires clusterid and limit, maxts and mints optional):
// * <pre>
// *   ./feed-reader hostname:port activity &lt;clusterid> &lt;limit> [maxts] [mints]
// * </pre>
// * <p>
// * Popular Usage (requires clusterid, numhours, and limit, offset optional):
// * <pre>
// *   ./feed-reader hostname:port popular &lt;clusterid> &lt;numhours> &lt;limit> [offset]
// * </pre>
// * <p>
// * Program just executes query via REST and returns output to stdout.
// */
//public class FeedReaderCLI {
//
//  private static void feedReadActivity(String baseUrl, int clusterid, int limit,
//      long maxts, long mints) {
//    String url = baseUrl + "readactivity?clusterid=" + clusterid + "&limit=" +
//      limit + "&maxts=" + maxts + "&mints=" + mints;
//    processUrl("activity feed", url);
//  }
//
//  private static void feedReadPopular(String baseUrl, int clusterid,
//      int numhours, int limit, int offset) {
//    String url = baseUrl + "readpopular?clusterid=" + clusterid + "&numhours=" +
//      numhours + "&limit=" + limit + "&offset=" + offset;
//      processUrl("popular feed", url);
//  }
//
//  private static void processUrl(String queryType, String url) {
//    try {
//      HttpResponse response = getUrl(url);
//      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
//        throw new IOException("Bad HTTP Response: " +
//            response.getStatusLine().getReasonPhrase());
//      }
//      int len = (int)response.getEntity().getContentLength();
//      byte [] bytes = new byte[len];
//      response.getEntity().getContent().read(bytes);
//      System.out.println(new String(bytes));
//    } catch (IOException e) {
//      System.err.println("IO/Network error retrieving " + queryType +
//          " using URL: " + url);
//      e.printStackTrace();
//    }
//  }
//
//  private static HttpResponse getUrl(String url)
//      throws ClientProtocolException, IOException {
//    HttpClient client = new DefaultHttpClient();
//    try {
//      return client.execute(new HttpGet(url));
//    } finally {
//      client.getConnectionManager().shutdown();
//    }
//  }
//  public static void main(String [] args) throws Exception {
//
//    if (args.length < 4)
//      throw new IllegalArgumentException("At least 4 args required");
//
//    // First arg must be 'activity' or 'popular'
//    // Second arg must be an integer for clusterid
//    // Rest of args depends on method.  Check for clusterid first, then branch
//    // on different feed types and do further parsing and call the exec method
//
//    String baseUrl = "http://" + args[0] + "/query/feedreader/";
//    int clusterid = parseInt(args[2]);
//
//    if (args[1].equals("activity")) {
//
//      // Requires limit, optionally contains maxts and mints
//      if (args.length > 6)
//        throw new IllegalArgumentException("Too many arguments");
//      int limit = parseInt(args[3]);
//      long maxts = args.length >= 5 ? parseLong(args[4]) : Long.MAX_VALUE;
//      long mints = args.length == 6 ? parseLong(args[5]) : 0;
//
//      feedReadActivity(baseUrl, clusterid, limit, maxts, mints);
//
//    } else if (args[1].equals("popular")) {
//
//      // Requires numhours and limit, optionally contains offset
//      if (args.length > 6)
//        throw new IllegalArgumentException("Too many arguments");
//      int numhours = parseInt(args[3]);
//      int limit = parseInt(args[4]);
//      int offset = args.length == 6 ? parseInt(args[5]) : 0;
//
//      feedReadPopular(baseUrl, clusterid, numhours, limit, offset);
//
//    } else {
//      // Invalid feed type
//      throw new IllegalArgumentException("Only activity and popular feeds " +
//          "supported, you gave " + args[1]);
//    }
//  }
//
//  private static int parseInt(String arg) throws NumberFormatException {
//    try {
//      int num = Integer.parseInt(arg);
//      return num;
//    } catch (NumberFormatException nfe) {
//      System.err.println("Argument [" + arg + " was excepted to be a number");
//      throw nfe;
//    }
//  }
//
//  private static long parseLong(String arg) throws NumberFormatException {
//    try {
//      long num = Long.parseLong(arg);
//      return num;
//    } catch (NumberFormatException nfe) {
//      System.err.println("Argument [" + arg + " was excepted to be a number");
//      throw nfe;
//    }
//  }
//}
