package com.continuuity.gateway.tools;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.gateway.collector.RestCollector;
import com.continuuity.gateway.util.Util;
import com.continuuity.gateway.util.HttpConfig;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

// todo document this
public class SendRestEvent {

  private static final Logger LOG = LoggerFactory
      .getLogger(SendRestEvent.class);

  /**
   * Retrieves the http config of the rest collector from the gateway
   * configuration. If no name is passed in, tries to figures out the
   * name by scanning through the configuration.
   *
   * @param config   The gateway configuration
   * @param restName The name of the rest collector, optional
   * @return The HttpConfig if found, or null otherwise.
   */
  static HttpConfig findRestConfig(CConfiguration config, String restName) {

    if (restName == null) {
      // find the name of the REST collector
      restName = Util.findConnector(config, RestCollector.class);
      if (restName == null) {
        return null;
      } else {
        LOG.info("Reading configuration for connector '" + restName + "'.");
      }
    }
    // get the collector's http config
    HttpConfig httpConfig = null;
    try {
      httpConfig = HttpConfig.configure(restName, config, null);
    } catch (Exception e) {
      LOG.error("Exception reading Http configuration for connector '"
          + restName + "': " + e.getMessage());
    }
    return httpConfig;
  }

  public static void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    out.println("Usage: SendRestEvent <option> ... with");
    out.println("  --base <url>            To specify the port to use");
    out.println("  --host <name>           To specify the hostname to send to");
    out.println("  --connector <name>      To specify the name of the rest collector");
    out.println("  --stream <name>         To specify the destination event stream");
    out.println("	 --header <name> <value> To specify a header for the event to send. Can be used multiple times");
    out.println("  --body <value>          To specify the body of the event");
    out.println("  --file <path>           To specify a file containing the body of the event");
    if (error) System.exit(1);
  }

  public static void main(String[] args) {

    String baseUrl = null;
    String hostname = "localhost";
    String connector = null;
    String filename = null;
    String destination = null;
    HttpPost post = new HttpPost();

    for (int pos = 0; pos < args.length; pos++) {
      String arg = args[pos];
      if ("--base".equals(arg)) {
        if (++pos >= args.length) usage(true);
        baseUrl = args[pos];
        continue;
      } else if ("--host".equals(arg)) {
        if (++pos >= args.length) usage(true);
        hostname = args[pos];
        continue;
      } else if ("--connector".equals(arg)) {
        if (++pos >= args.length) usage(true);
        connector = args[pos];
        continue;
      } else if ("--stream".equals(arg)) {
        if (++pos >= args.length) usage(true);
        destination = args[pos];
        continue;
      } else if ("--header".equals(arg)) {
        if (pos + 2 >= args.length) usage(true);
        post.setHeader(args[++pos], args[++pos]);
        continue;
      } else if ("--body".equals(arg)) {
        if (++pos >= args.length) usage(true);
        post.setEntity(new ByteArrayEntity(args[pos].getBytes()));
        continue;
      } else if ("--file".equals(arg)) {
        if (++pos >= args.length) usage(true);
        filename = args[pos];
        continue;
      } else if ("--help".equals(arg)) {
        usage(false);
        System.exit(0);
      }
      // unkown argument
      usage(true);
    }

    if (baseUrl == null) {
      CConfiguration config = CConfiguration.create();
      HttpConfig httpConfig = findRestConfig(config, connector);
      if (httpConfig != null) {
        baseUrl = httpConfig.getBaseUrl(hostname);
      }
    }
    if (baseUrl == null) {
      System.err.println("Can't figure out the URL to send to. Please use --base or --connector to specify.");
      System.exit(1);
    } else {
      System.out.println("Using base URL: " + baseUrl);
    }

    // add the stream name to the baseUrl
    if (destination == null) {
      System.err.println("Need a stream name to form REST url.");
      System.exit(1);
    }

    String url = baseUrl + destination;
    try {
      URI uri = new URI(url);
      post.setURI(uri);
    } catch (URISyntaxException e) {
      System.err.println("URI " + url + " is not well-formed. Giving up.");
      System.exit(1);
    }

    if (filename != null) {
      byte[] bytes = Util.readBinaryFile(filename);
      if (bytes == null) {
        System.err.println("Cannot read body from file " + filename + ".");
        System.exit(1);
      }
      post.setEntity(new ByteArrayEntity(bytes));
    }

    if (post.getEntity() == null) {
      System.err.println("Cannot send an event without body. Please use --body or --file to specify the body.");
      System.exit(1);
    }

    // post is now fully constructed, ready to send
    HttpClient client = new DefaultHttpClient();
    try {
      HttpResponse response = client.execute(post);
      int status = response.getStatusLine().getStatusCode();
      if (status != HttpStatus.SC_OK) {
        System.err.println("Error sending event: " + response.getStatusLine());
        System.exit(1);
      }
    } catch (IOException e) {
      System.err.println("Exception when sending event: " + e.getMessage());
    } finally {
      client.getConnectionManager().shutdown();
    }
  }
}
