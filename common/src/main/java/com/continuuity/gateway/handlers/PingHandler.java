package com.continuuity.gateway.handlers;

import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;

/**
 * Handles ping requests.
 */
public class PingHandler extends AbstractHttpHandler {

  @Path("/ping")
  @GET
  public void ping(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, "OK.\n");
  }


  @Path("/status")
  @GET
  public void status(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, "OK.\n");
  }
}
