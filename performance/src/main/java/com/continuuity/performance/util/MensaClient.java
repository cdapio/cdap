package com.continuuity.performance.util;

import com.google.gson.GsonBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Properties;
/**
 * Client to read data from Mensa.
 */
public class MensaClient {
  private static final String DUMMY_OP = "avg:";
  private static final String DEFAULT_START_TS = "2000/01/01-00:00:00";

  String command;
  String host;
  int port;
  String metric;
  Properties config;
  Properties tags;
  String function;
  String startts;
  String endts;
  DateTime now;

  public MensaClient() {
    this.tags = new Properties();
    this.config = new Properties();
    this.now = new org.joda.time.DateTime();
  }

  private String buildTsdbQuery(String start, String end, String metric) {
    // returns /q?start=2013/04/15-01:50:00&
    StringBuilder sb = new StringBuilder();
    sb.append("/q?");
    if (start != null && start.length() != 0) {
      sb.append("start=" + start);
      sb.append("&");
    }
    if (end != null && end.length() != 0) {
      sb.append("end=" + end);
      sb.append("&");
    }
    sb.append("m=");
    sb.append(DUMMY_OP);
    sb.append(metric);
    if (tags != null && tags.size() != 0) {
      sb.append(tags.toString().replaceAll(" ", ""));
    }
    sb.append("&");
    sb.append("ascii");
    sb.append("&");
    sb.append("njson");
    return sb.toString();
  }

  private HttpGet buildHttpGet(String hostname, int port, String query) {
    try {
      URL url = new URL("http://" + hostname + ":" + port + query);
      URI uri = new URI(url.getProtocol(), url.getHost() + ":" + url.getPort(), url.getPath(), url.getQuery(), null);
      HttpGet get = new HttpGet(uri);
      get.addHeader("accept", "application/json");
      return get;
    } catch (MalformedURLException e) {
      e.printStackTrace();
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return null;
  }

  public String round(double value) {
    return String.format("%1.2f", value);
  }

  public String execute0() {
    HttpClient client = new DefaultHttpClient();
    HttpResponse response;

    if ("create".equals(command)) {
      return "OK.";

    } else if ("read".equals(command)) {
      try {

        String tsdbQuery = buildTsdbQuery(startts, endts, metric);
        HttpGet get = buildHttpGet(host, port, tsdbQuery);

        response = client.execute(get);
        HttpEntity entity = response.getEntity();
        MetricsResult result;
        if (entity != null) {
          InputStream is = entity.getContent();
          try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));

            result = new GsonBuilder()
              .registerTypeAdapter(MetricsResult.Metric.DataPoint.class,
                                   new MetricsResult.Metric.DataPointDeserializer())
              .create().fromJson(reader, MetricsResult.class);

            if (function.startsWith("raw")) {
              if ("raw".equals(function)) {
                result.getMetric(0).dump();
              } else {
                System.out.println(function + "="
                                     + round(result.getMetric(0).avg(Integer.valueOf(function.substring(3)))));
              }
            } else if (function.startsWith("avg")) {
              if ("avg".equals(function)) {
                System.out.println(function + "=" + round(result.getMetric(0).avg()));
              } else {
                System.out.println(function + "="
                                     + round(result.getMetric(0).avg(Integer.valueOf(function.substring(3)))));
              }
            }
          } catch (RuntimeException ex) {
            get.abort();
            throw ex;
          } finally {
            is.close();
          }
          client.getConnectionManager().shutdown();
        }
      } catch (IOException e) {
        System.err.println("Error sending HTTP request: " + e.getMessage());
        return null;
      }

    } else if ("delete".equals(command)) {
      return "OK.";
    } else if ("list".equals(command)) {
      return "OK.";
    }
    return "OK.";
  }
  public String execute() {
    try {
      return execute0();
    } catch (com.continuuity.common.utils.UsageException e) {
        System.err.println("Exception: " + e);
        e.printStackTrace(System.err);
    }
    return null;
  }

  private void parseTags(String tagsString, String tagSeparator,  Properties tags) {
    for (String tag : tagsString.trim().split(tagSeparator)) {
      parseTag(tag.trim(), tags);
    }
  }

  private void parseTag(String tag, Properties tags) {
    String[] tagCatVal = tag.split("=");
    tags.setProperty(tagCatVal[0], tagCatVal[1]);
  }

  void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = "mensa-client";
    if (System.getProperty("script") != null) {
      name = System.getProperty("script").replaceAll("[./]", "");
    }
    out.println("Usage: ");
    out.println("  " + name + " read <metric id> [ <option> ... ]");
    out.println("Options:");
    out.println("  --host <name>            To specify the hostname to send to");
    out.println("  --function <id>          To specify an aggregation function");
    out.println("  --tag <name=value>       To specify a tag with name and value");
    out.println("  --ts <timestamp>         To specify a timestamp");
    out.println("  --epoch <timestamp>      To specify the Unix epoch");
    out.println("  --startts <timestamp>    To specify the start timestamp");
    out.println("  --endts <timestamp>      To specify the end timestamp");
    out.println("  --startepoch <epoch>     To specify the start Unix epoch");
    out.println("  --endepoch <epoch>       To specify the end Unix epoch");
    out.println("  --first <number>         To read the first N data points of the metric");
    out.println("  --last <number>          To read the last N data points of the metric.");
    out.println("  --help                   To print this message");
    out.println("Examples:");
    out.println("  " + name + " read io.bytes --ts 2013/04/15-01:55:00 --tag host=server1");
    out.println("  " + name + " read io.bytes --ts 2013/04/15-01:55:00 --tags host=server1,iomode=read");
    if (error) {
      throw new UsageException();
    }
  }

  private boolean parseArgs(String[] args) {
    if (args.length == 0) {
      usage(true);
    }
    int pos = 0;
    String arg = args[pos];

    if ("--help".equals(arg)) {
      usage(false);
      return false;
    }

    command = arg;
    //parse command
    if ("read".equals(command)) {
      if ((pos + 1) >= args.length) {
        usage(true);
      }
      metric = args[ ++pos ];

    } else {
      command = null;
    }

    //parse options
    while (pos < args.length - 1) {
      arg = args[ ++pos ];
      if ("--tag".equals(arg)) {
        //--tag name=value
        if ((pos + 1) >= args.length) {
          usage(true);
        }
        parseTag(args[ ++pos ], tags);

      } else if ("--tags".equals(arg)) {
        //--tags name1=value1,name2=value2,name3=value3
        if ((pos + 1) >= args.length) {
          usage(true);
        }
        parseTags(args[ ++pos ], ",", tags);

      } else if ("--startts".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        startts = args[pos];

      } else if ("--endts".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        endts = args[pos];

      } else if ("--host".equals(arg)) {
        if (++pos >= args.length) { usage(true); }
        host = args[pos];

      } else if ("--port".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        try {
          port = Integer.valueOf(args[pos]);
        } catch (NumberFormatException e) {
          usage(true);
        }

      } else if ("--function".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
        function = args[pos];

      } else { // unkown argument
        usage(false);
        return false;
      }
    }

    if (startts == null) {
      startts = DEFAULT_START_TS;
    }

    if (endts == null) {
      DateTimeFormatter fmt = DateTimeFormat.forPattern("YYYY/MM/dd-HH:mm:ss");
      endts = fmt.print(now);
    }

    if (command == null || command.length() == 0 || host == null || host.length() == 0 || port == 0) {
      return false;
    }
    return true;
  }

  public static void main(String[] args) {
    MensaClient mc = new MensaClient();
    mc.parseArgs(args);
    mc.execute();
  }

  /**
   * UsageException.
   */
  public class UsageException extends RuntimeException {
    // no message, no cause, on purpose, only default constructor
    public UsageException() { }
  }
}
