package com.continuuity.gateway.handlers;

import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.google.common.base.Charsets;
import com.google.inject.Inject;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import com.ning.http.client.providers.netty.NettyAsyncHttpProvider;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;

/**
 * Client to make calls to workflow http service and return the status.
 */
public class WorkflowClient {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowClient.class);
  private final AsyncHttpClient httpClient;
  private final DiscoveryServiceClient discoveryServiceClient;
  @Inject
  WorkflowClient(DiscoveryServiceClient discoveryServiceClient) {
    this.discoveryServiceClient = discoveryServiceClient;
    AsyncHttpClientConfig.Builder configBuilder = new AsyncHttpClientConfig.Builder();
    this.httpClient = new AsyncHttpClient(new NettyAsyncHttpProvider(configBuilder.build()),
                                               configBuilder.build());
  }

  public void getWorkflowStatus(String accountId, String appId, String workflowId, final Callback callback)
                      throws IOException, ExecutionException, InterruptedException {
    // determine the service provider for the given path
    String serviceName = String.format("workflow.%s.%s.%s", accountId, appId, workflowId);
    Discoverable discoverable = new RandomEndpointStrategy(discoveryServiceClient.discover(serviceName)).pick();

    if (discoverable == null) {
      LOG.debug("No endpoint for service {}", serviceName);
      callback.handle(new Status(Status.Code.NOT_FOUND, ""));
      return;
    }

    // make HTTP call to workflow service.
    InetSocketAddress endpoint = discoverable.getSocketAddress();
    // Construct request
    String url = String.format("http://%s:%d/status", endpoint.getHostName(), endpoint.getPort());
    Request workflowRequest = new RequestBuilder("GET").setUrl(url).build();

    httpClient.executeRequest(workflowRequest, new AsyncCompletionHandler<Void>() {
                                @Override
                                public Void onCompleted(Response response) throws Exception {
                                  callback.handle(new Status(Status.Code.OK,
                                                            response.getResponseBody(Charsets.UTF_8.name())));
                                  return null;
                                }

                                @Override
                                public void onThrowable(Throwable t) {
                                  LOG.warn("Failed to request for workflow status", t);
                                  callback.handle(new Status(Status.Code.ERROR, ""));

                                }

    });

  }

  /**
   * POJO to represent status of http call to workflow service.
   */
  public static class Status {
    /**
     * Status code.
     */
    public enum Code { NOT_FOUND, OK, ERROR }

    private final Code code;

    private final String result;

    public Status(Code code, String result) {
      this.code = code;
      this.result = result;
    }

    public Code getCode() {
      return code;
    }

    public String getResult() {
      return result;
    }
  }

  /**
   * Callback to implement to handle WorkflowStatus.
   */
  public static interface Callback {

    /**
     * Handle to implement the status from workflow client.
     *
     * @param status status of the call to http workflow service.
     */
    void handle(Status status);

  }

}
