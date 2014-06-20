package com.continuuity.explore.executor;

import com.continuuity.common.conf.Constants;
import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;

import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;


/**
 * Explore ping handler - reachable outside of Reactor.
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class ExplorePingHandler extends AbstractHttpHandler {

  @Path("/explore/status")
  @GET
  public void status(@SuppressWarnings("UnusedParameters") HttpRequest request, HttpResponder responder) {
    responder.sendString(HttpResponseStatus.OK, "OK.\n");
  }
}
