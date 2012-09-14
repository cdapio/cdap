package com.continuuity.metrics2.collector.server.plugins;

import akka.dispatch.ExecutionContext;
import akka.dispatch.ExecutionContexts;
import akka.dispatch.Future;
import akka.dispatch.Futures;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics2.collector.MetricRequest;
import com.continuuity.metrics2.collector.MetricResponse;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Realm;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import com.ning.http.util.Base64;
import com.sun.jersey.core.util.StringIgnoreCaseKeyComparator;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

/**
 *
 */
public class LibratoMetricsProcessor implements MetricsProcessor {

  /**
   * Execution context under which the DB updates will happen.
   */
  private final ExecutionContext ec
    = ExecutionContexts.fromExecutorService(Executors.newCachedThreadPool());

  /**
   * Specifies the user agent.
   */
  private static final String userAgent;

  static {
    String libVersion = "1.1";
    String agentIdentifier = "librato-reporter";
    userAgent = String.format("%s librato-java/%s",
                              agentIdentifier, libVersion);
  }

  /**
   * Instance of async http client.
   */
  private final AsyncHttpClient client = new AsyncHttpClient();

  /**
   * Instance of JSON object mapper.
   */
  private final ObjectMapper mapper = new ObjectMapper();

  /**
   * Account ID to be used for sending metrics to librato.
   */
  private String libratoAccount;

  /**
   * Token to be passed for auth.
   */
  private String libratoToken;

  /**
   * URL to send metrics to.
   */
  private String libratoUrl;

  public LibratoMetricsProcessor(CConfiguration configuration) {
    libratoAccount = configuration.get(
      "librato.account.name", "nitin@continuuity.com"
    );
    libratoToken = configuration.get(
      "librato.account.token",
      "6b8ac685e4665f78ba1f8f10b0c509b054fb92f96d711fe8f7505225064f81e1"
    );
    libratoUrl = configuration.get(
      "librato.url", "https://metrics-api.librato.com/v1/metrics"
    );
  }

  /**
   * Processes a {@link com.continuuity.metrics2.collector.MetricRequest}.
   * <p/>
   * <p>
   * Processing a metric returns a Future (an object holding the future of
   * processing).
   * </p>
   *
   * @param request that needs to be processed.
   * @return Future for asynchronous processing of metric.
   * @throws java.io.IOException
   */
  @Override
  public Future<MetricResponse.Status> process(MetricRequest request)
    throws IOException {

    // Serialize the metrics into the format needed.
    Map<String, Object> metric = new HashMap<String, Object>();
    metric.put("source", request.getTags().get(0).getSecond());
    metric.put("measure_time", request.getTimestamp());
    metric.put("period", 1);
    metric.put("value", request.getValue());

    Map<String, Object> metrics = new HashMap<String, Object>();
    Map<String, Object> guages = new HashMap<String, Object>();
    guages.put(request.getMetricName(), metric);
    metrics.put("gauges", guages);

    // Write the body.
    final RequestBuilder builder = new RequestBuilder("POST");
    String json = mapper.writeValueAsString(metrics);
    builder.setBody(json);
    builder.addHeader("Content-Type", "application/json");
    builder.setHeader("User-Agent", userAgent);
    builder.setHeader("Keep-Alive", "true");
    builder.addHeader("Authorization",
                      String.format("Basic %s",
                                    Base64.encode(
                                      (libratoAccount + ":" + libratoToken)
                                      .getBytes()
                                    )
                      )
    );
    builder.setUrl(libratoUrl);

    // Do the work in callable and return future.
    return Futures.future(new Callable<MetricResponse.Status>() {
      @Override
      public MetricResponse.Status call() throws Exception {
        java.util.concurrent.Future<Response> response
          = client.executeRequest(builder.build());
        try {
          Response r = response.get();
          if(r.getStatusCode() != 200) {
            return MetricResponse.Status.FAILED;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return MetricResponse.Status.FAILED;
        } catch (ExecutionException e) {
          return MetricResponse.Status.FAILED;
        }
        return MetricResponse.Status.SUCCESS;
      }
    }, ec);
  }

  @Override
  public void close() throws IOException {
    if(client != null) {
      client.close();
    }
  }
}
