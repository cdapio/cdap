package com.continuuity.metrics2.collector.server.plugins;

import akka.dispatch.ExecutionContext;
import akka.dispatch.ExecutionContexts;
import akka.dispatch.Future;
import akka.dispatch.Futures;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.metrics2.collector.MetricRequest;
import com.continuuity.metrics2.collector.MetricResponse;
import com.google.common.util.concurrent.AbstractScheduledService;
import com.ning.http.client.*;
import com.ning.http.util.Base64;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Processor plugin for sending the metrics received to librato using
 * the librato RESTful APIs. Documentation for librato APIs are available
 * here http://dev.librato.com/v1/metrics.
 */
public final class LibratoMetricsProcessor implements MetricsProcessor {
  private static final Logger Log
    = LoggerFactory.getLogger(LibratoMetricsProcessor.class);

  /**
   * Static block for initializing a few things needed.
   */
  static {
    String libVersion = "1.1";
    String agentIdentifier = "librato-reporter";
    userAgent = String.format("%s librato-java/%s",
                              agentIdentifier, libVersion);
  }

  /**
   * Executor service instance.
   */
  private final ExecutorService es = Executors.newFixedThreadPool(50);

  /**
   * Execution context under which the DB updates will happen.
   */
  private final ExecutionContext ec
    = ExecutionContexts.fromExecutorService(es);

  /**
   * Specifies the user agent.
   */
  private static final String userAgent;

  /**
   * Maximum number of elements in queue before they
   * are sent to librato.
   */
  private static final int QUEUE_LENGTH = 100000;

  /**
   * Size of batch to be sent to librato.
   */
  private static final int BATCH_SIZE = 1000;

  /**
   * Maximum time a request can take.
   */
  private static final int LIBRATO_HTTP_REQUEST_TIMEOUT_MS = 2000;

  /**
   * Maximum time to connect to librato service.
   */
  private static final int LIBRATO_HTTP_CONNECT_TIMEOUT_MS = 1000;

  /**
   * Maximum time a connection is pool is idle.
   */
  private static final int LIBRATO_HTTP_IDLE_CONNECTION_IN_POOL_TIMEOUT_MS
    = 30000;

  /**
   * Specifies the amount of time to wait till the queue is available
   * to insert the data point.
   */
  private static final int QUEUE_OFFER_TIMEOUT_SECONDS = 2;

  /**
   * Instance of async http client with default timeout across
   * all the requests made to librato.
   */
  private final AsyncHttpClient client = new AsyncHttpClient(
    new AsyncHttpClientConfig.Builder()
      .setRequestTimeoutInMs(LIBRATO_HTTP_REQUEST_TIMEOUT_MS)
      .setConnectionTimeoutInMs(LIBRATO_HTTP_CONNECT_TIMEOUT_MS)
      .setCompressionEnabled(true)
      .setIdleConnectionInPoolTimeoutInMs(
        LIBRATO_HTTP_IDLE_CONNECTION_IN_POOL_TIMEOUT_MS
      )
      .build()
  );

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

  /**
   * Queue.
   */
  private static LinkedBlockingQueue<MetricRequest> queue
    = new LinkedBlockingQueue<MetricRequest>(QUEUE_LENGTH);

  /**
   * A batching service handler that's responsible for creating a
   * batch of requests to be made librato.
   */
  private LibratoBatcher libratoBatcher = new LibratoBatcher();

  private class LibratoBatcher extends AbstractScheduledService {
    private int attemptCount = 0;

    /**
     * Run one iteration of the scheduled task. If any invocation of this
     * method throws an exception,
     * the service will transition to the {@link com.google.common.util
     * .concurrent.Service.State#FAILED} state and this method will no
     * longer be called.
     */
    @Override
    protected void runOneIteration() throws Exception {
      try {
        // We increase the attempt count.
        attemptCount++;

        // If we have reached the size or we have attempted to
        // send the metrics and have not been due to length of
        // queue not reaching the limit.
        if(queue.size() < BATCH_SIZE && attemptCount < 10) {
          return;
        }

        // Reset the counter.
        attemptCount = 0;

        List<Object> metrics = new ArrayList<Object>();
        int count = Math.min(queue.size(), BATCH_SIZE);


        Log.debug("Current queue size {}, sending metrics to librato",
                  count);

        // If there is nothing in queue, then we don't need to
        // send anything to librato.
        if(count < 1) {
          return;
        }

        Log.debug("Sending a batch of {} to librato.", count);

        // Iterate through the queue and generate the body of the
        // request to be sent to librato.
        while(!queue.isEmpty() && count > 0) {
          MetricRequest request = queue.poll();
          if(request == null) {
            break;
          }
          // Serialize the metrics into the format needed.
          Map<String, Object> metric = new HashMap<String, Object>();
          metric.put("name", request.getMetricName());
          metric.put("source", request.getTags().get(0).getSecond());
          metric.put("measure_time", request.getTimestamp());
          metric.put("period", 1);
          metric.put("value", request.getValue());
          metrics.add(metric);
          count--;
        }

        Map<String, Object> guages = new HashMap<String, Object>();
        guages.put("gauges", metrics);

        // Write the body and prepare the request to be sent.
        final RequestBuilder builder = new RequestBuilder("POST");
        String json = mapper.writeValueAsString(guages);
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

        java.util.concurrent.Future<Response> response
          = client.executeRequest(builder.build());
        Response r = response.get();
        if(r.getStatusCode() != 200) {
          Log.warn("Failed sending metric to librato. Status code {}, status {}",
                   r.getStatusCode(), r.getStatusText());
        } else {
          Log.debug("Successfully sent metric to librato.");
        }
      } catch (IOException e) {
        Log.warn("I/O Exception. Reason : {}", e.getMessage());
      } catch (IllegalArgumentException e) {
        Log.warn("Illegal argument exception. Reason : {}", e.getMessage());
      } catch (InterruptedException e) {
        Log.warn("Thread interrupted. Reason : {}", e.getMessage());
        Thread.currentThread().interrupt();
      } catch (ExecutionException e) {
        Log.warn("Execution exception. Reason : {}", e.getMessage());
      }
    }

    /**
     * Returns the {@link com.google.common.util.concurrent
     * .AbstractScheduledService.Scheduler} object used to configure this
     * service.  This method will only be
     * called once.
     */
    @Override
    protected Scheduler scheduler() {
      return Scheduler.newFixedRateSchedule(0, 100, TimeUnit.MILLISECONDS);
    }
  }

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

    // We don't need to wait for future.
    libratoBatcher.start();
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
  public Future<MetricResponse.Status> process(final MetricRequest request)
    throws IOException {

    // Check if librato batcher is running, if not then we return
    // appropriate server side error to client.
    if(! libratoBatcher.isRunning()) {
      return Futures.future(new Callable<MetricResponse.Status>() {
        @Override
        public MetricResponse.Status call() throws Exception {
          Log.error("Server error. Librato batcher is not running.");
          return MetricResponse.Status.SERVER_ERROR;
        }
      }, ec);
    }

    // We add it to queue and because adding it to queue can block
    // we return future. This could become an issue on the server
    // side if the queue is blocked for long periods of time.
    return Futures.future(new Callable<MetricResponse.Status>() {
      @Override
      public MetricResponse.Status call() throws Exception {
        try {
          if(! queue.offer(request, QUEUE_OFFER_TIMEOUT_SECONDS,
                           TimeUnit.SECONDS)) {
            Log.warn("Ignoring data point. Queue is full, timeout {}s reached.",
                     QUEUE_OFFER_TIMEOUT_SECONDS);
            return MetricResponse.Status.IGNORED;
          }
        } catch (InterruptedException e) {
          return MetricResponse.Status.IGNORED;
        }
        Log.debug("Successfully added metric {} to librato queue.",
                  request.toString());
        return MetricResponse.Status.SUCCESS;
      }
    }, ec);
  }

  @Override
  public void close() throws IOException {
    if(client != null) {
      client.close();
    }
    if(es != null) {
      es.shutdown();
    }
  }
}
