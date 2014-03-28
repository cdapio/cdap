/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.workflow;

import com.continuuity.api.workflow.WorkflowActionSpecification;
import com.continuuity.app.runtime.workflow.WorkflowStatus;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.internal.app.WorkflowActionSpecificationCodec;
import com.google.common.base.Supplier;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * A HttpHandler for handling Workflow REST API.
 */
public final class WorkflowServiceHandler extends AbstractHttpHandler {

  private static final Gson GSON = new GsonBuilder()
                                    .registerTypeAdapter(WorkflowActionSpecification.class,
                                                         new WorkflowActionSpecificationCodec())
                                    .create();

  private final Supplier<WorkflowStatus> statusSupplier;

  WorkflowServiceHandler(Supplier<WorkflowStatus> statusSupplier) {
    this.statusSupplier = statusSupplier;
  }

  /**
   * Provides response to {@code /status} call to gives the latest status of this workflow.
   */
  @GET
  @Path("/status")
  public void handleStatus(HttpRequest request, HttpResponder responder) {
    responder.sendJson(HttpResponseStatus.OK, statusSupplier.get(), WorkflowStatus.class, GSON);
  }
}
