package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.weave.discovery.Discoverable;
import com.continuuity.weave.discovery.DiscoveryServiceClient;
import com.google.common.base.Charsets;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Client to make calls to workflow http service and return the status.
 */
public class WorkflowClient {

  private static Logger LOG = LoggerFactory.getLogger(WorkflowClient.class);
  private static AsyncHttpClient httpClient = new AsyncHttpClient();

  public static Status getWorkflowStatus(DiscoveryServiceClient discoveryServiceClient,
                                       String accountId, String appId, String workflowId)
                      throws IOException, ExecutionException, InterruptedException {
    // determine the service provider for the given path
    String serviceName = String.format("workflow.%s.%s.%s", accountId, appId, workflowId);
    Discoverable discoverable = new RandomEndpointStrategy(discoveryServiceClient.discover(serviceName)).pick();

    if (discoverable == null) {
      LOG.debug("No endpoint for service {}", serviceName);
      return new Status(Status.Code.NOT_FOUND, "");
    }

    // make HTTP call to workflow service.
    InetSocketAddress endpoint = discoverable.getSocketAddress();
    // Construct request
    String url = String.format("http://%s:%d/status", endpoint.getHostName(), endpoint.getPort());
    Request workflowRequest = new RequestBuilder("GET").setUrl(url).build();

    Future<Response> response = httpClient.executeRequest(workflowRequest,
                                                          new AsyncCompletionHandler<Response>() {
                                @Override
                                public Response onCompleted(Response response) throws Exception {
                                  return response;
                                }

                                @Override
                                public void onThrowable(Throwable t) {
                                  LOG.warn("Failed to request for workflow status", t);
                                }
    });
               int code = response.get().getStatusCode();
    if (response.get().getStatusCode() == HttpResponseStatus.OK.getCode()) {
      return new Status(Status.Code.OK, response.get().getResponseBody(Charsets.UTF_8.name()));
    } else {
      return new Status(Status.Code.ERROR, "");
    }
  }

  /**
   * POJO to represent status of http call to workflow service.
   */
  public static class Status {
    /**
     * Status code.
     */
    public enum Code {NOT_FOUND, OK, ERROR}

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

}
