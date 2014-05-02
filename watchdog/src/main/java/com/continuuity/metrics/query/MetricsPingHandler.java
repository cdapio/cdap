package com.continuuity.metrics.query;

import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Ping Handler in Metrics
 */
public class MetricsPingHandler extends AbstractHttpHandler {
  @Path("/ping")
  @GET
  public void ping(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, "OK.\n");
  }
}
