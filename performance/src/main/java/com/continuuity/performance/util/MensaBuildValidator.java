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
import java.io.DataInputStream;
import java.io.FileInputStream;
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
 * Tool to compare results from latest with previous benchmarks.
 */
public class MensaBuildValidator {
  private static final String DUMMY_OP = "avg:";
  private static final String DEFAULT_START_TS = "2000/01/01-00:00:00";

  String command;
  String host;
  int port;
  String reportFileName;
  Properties config;
  Properties tags;
  String startts;
  String endts;
  DateTime now;

  public MensaBuildValidator() {
    this.tags = new Properties();
    this.config = new Properties();
    this.now = new DateTime();
  }

  private String buildTsdbQuery(String start, String end, String metric, String tags) {
    //returns i.e. /q?start=2013/04/15-01:50:00&
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
    if (tags != null) {
      sb.append("{");
      sb.append(tags);
      sb.append("}");
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

  public MetricsResult queryTSDB(String host, int port, String startts, String endts, String metric, String tags) {
    MetricsResult result = null;
    HttpClient client = new DefaultHttpClient();
    HttpResponse response;

    try {
      String tsdbQuery = buildTsdbQuery(startts, endts, metric, tags);
      HttpGet get = buildHttpGet(host, port, tsdbQuery);

      response = client.execute(get);
      HttpEntity entity = response.getEntity();

      if (entity != null) {
        InputStream is = entity.getContent();
        try {
          BufferedReader reader = new BufferedReader(new InputStreamReader(is));

          result = new GsonBuilder().registerTypeAdapter(MetricsResult.Metric.DataPoint.class, new MetricsResult
            .Metric.DataPointDeserializer())
            .create().fromJson(reader, MetricsResult.class);

        } catch (RuntimeException ex) {
          get.abort();
          throw ex;
        } finally {
          is.close();
        }
        client.getConnectionManager().shutdown();
      }
    } catch (IOException e) {
      System.err.println("Error sending HTTP request: "  +  e.getMessage());
      return null;
    }

    return result;
  }


  public boolean validate(String reportLine) {
    String[] split = reportLine.split(" ");
    String metric = split[1];
    double newMetricValueAvg = Double.valueOf(split[3]);
    StringBuilder sb = new StringBuilder();
    for (int i = 4; i < split.length; i++) {
      if (!split[i].contains("build")) {
        sb.append(split[i]);
        sb.append(",");
      }
    }
    sb.setLength(sb.length() - 1);
    String tags = sb.toString();
    MetricsResult result = queryTSDB(host, port, startts, endts, metric, tags);
    double oldMetricValueAvg = 0;
    if (result != null && result.getMetrics().size() != 0 && result.getMetric(0).getNumDataPoints() >= 7) {
      oldMetricValueAvg = result.getMetric(0).avg(7);
    }
    if (newMetricValueAvg < 0.95 * oldMetricValueAvg) {
      System.out.println("Mensa build validation failure! old avg metric value is " + oldMetricValueAvg
                         + ", new avg value is " + newMetricValueAvg);
      return false;
    } else {
      return true;
    }
  }

  public boolean execute() throws IOException {
    BufferedReader br = new BufferedReader(
      new InputStreamReader(
        new DataInputStream(
          new FileInputStream(reportFileName))));
    String reportLine;
    try {
      while ((reportLine = br.readLine()) != null)   {
        if (validate(reportLine) == false) {
          return false;
        }
      }
    } finally {
      br.close();
    }
    return true;
  }

  void usage(boolean error) {
    PrintStream out = (error ? System.err : System.out);
    String name = "mensa-build-validator";
    if (System.getProperty("script") != null) {
      name = System.getProperty("script").replaceAll("[./]", "");
    }
    out.println("Usage: ");
    out.println("  " + name + " validate <build report file name> [ <option> ... ]");
    out.println("Options:");
    out.println("  --host <name>            To specify the hostname of the mensa server");
    out.println("  --port <number>          To specify the port number of the mensa server");
    out.println("  --help                   To print this message");
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
    if ("validate".equals(command)) {
      if ((pos + 1) >= args.length) {
        usage(true);
      }
      reportFileName = args[ ++pos ];
    } else {
      command = null;
    }

    //parse options
    while (pos < args.length - 1) {
      arg = args[ ++pos ];
      if ("--host".equals(arg)) {
        if (++pos >= args.length) {
          usage(true);
        }
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

  public static void main(String[] args) throws IOException {
    MensaBuildValidator mbv = new MensaBuildValidator();
    mbv.parseArgs(args);
    if (!mbv.execute()) {
      System.exit(1);
    }
  }

  /**
   * UsageException.
   */
  public class UsageException extends RuntimeException {
    // no message, no cause, on purpose, only default constructor
    public UsageException() { }
  }
}
