/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.gateway.v2.handlers.v2;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.google.common.collect.ImmutableMultimap;
import com.google.inject.Inject;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.providers.netty.NettyAsyncHttpProvider;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;


/**
 * A HttpHandler for gateway to pipe request to running Workflow.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public final class WorkflowHandler extends AuthenticatedHttpHandler {

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowHandler.class);
  private final WorkflowClient workflowClient;
  private final AsyncHttpClient asyncHttpClient;

  @Inject
  public WorkflowHandler(GatewayAuthenticator authenticator, WorkflowClient workflowClient) {
    super(authenticator);
    this.workflowClient = workflowClient;

    AsyncHttpClientConfig.Builder configBuilder = new AsyncHttpClientConfig.Builder();
    this.asyncHttpClient = new AsyncHttpClient(new NettyAsyncHttpProvider(configBuilder.build()),
                                               configBuilder.build());
  }

  @Override
  public void init(HandlerContext context) {
    LOG.info("Starting WorkflowHandler.");
  }

  @Override
  public void destroy(HandlerContext context) {
    LOG.info("Stopping WorkflowHandler.");
    asyncHttpClient.close();
  }

  @GET
  @Path("/apps/{app-id}/workflows/{workflow-name}/current")
  public void workflowStatus(HttpRequest request, final HttpResponder responder,
                             @PathParam("app-id") String appId, @PathParam("workflow-name") String workflowName) {

    try {
      String accountId = getAuthenticatedAccountId(request);
      workflowClient.getWorkflowStatus(accountId, appId, workflowName,
                                       new WorkflowClient.Callback() {
                                      @Override
                                      public void handle(WorkflowClient.Status status) {
                                        if (status.getCode() == WorkflowClient.Status.Code.NOT_FOUND) {
                                          responder.sendStatus(HttpResponseStatus.NOT_FOUND);
                                        } else if (status.getCode() == WorkflowClient.Status.Code.OK) {
                                          responder.sendByteArray(HttpResponseStatus.OK, status.getResult().getBytes(),
                                                                  ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE,
                                                                                   "application/json; charset=utf-8"));

                                        } else {
                                          responder.sendError(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                                                              status.getResult());
                                        }
                                      }
                                    });
    } catch (Throwable e) {
      LOG.error("Caught exception", e);
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }
}
