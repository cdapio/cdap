/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.handlers;

import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.core.service.SecurityService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

/**
 * Handle Nonce operation for Session nonce.
 */
@Path("/passport/v1/sso/")
@Singleton
public class SessionNonceHandler extends PassportHandler implements HttpHandler {

  private final SecurityService securityService;
  private static final Logger LOG = LoggerFactory.getLogger(SessionNonceHandler.class);

  @Inject
  public SessionNonceHandler(SecurityService securityService) {
    this.securityService = securityService;
  }


  @Path("{id}/generateNonce")
  @POST
  @Produces("application/json")
  public void getSessionNonce(HttpRequest request, HttpResponder responder,
                              @PathParam("id") String id) {
    requestReceived();
    int nonce = -1;
    try {
      nonce = securityService.getSessionNonce(id);
      if (nonce != -1) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, Utils.getNonceJson(null, nonce));
      } else {
        requestFailed();
        LOG.error(String.format("Failed to generate nonce. Endpoint %s", "GET /passport/v1/sso/getNonce/{id}"));
        responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getNonceJson("Couldn't generate nonce", nonce));
      }
    } catch (RuntimeException e) {
      requestFailed();
      LOG.error(String.format("Failed to generate nonce. Endpoint %s %s",
                              "GET /passport/v1/sso/getNonce/{id}", e.getMessage()));
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           Utils.getNonceJson("Couldn't generate nonce", nonce));
    }
  }

  @Path("{nonce}/getId")
  @GET
  @Produces("application/json")
  public void getSessionId(HttpRequest request, HttpResponder responder,
                           @PathParam("nonce") int nonce) {
    requestReceived();
    String id = null;
    try {
      id = securityService.getSessionId(nonce);
      if ((id != null) && (!id.isEmpty())) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, Utils.getIdJson(null, id));
      } else {
        requestFailed();
        LOG.error(String.format("Failed to generate sessionId. Endpoint %s",
          "GET /passport/v1/sso/getId/{nonce}"));
        responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getIdJson("ID not found for nonce", null));
      }
    } catch (StaleNonceException e) {
      requestFailed();
      LOG.error(String.format("Failed to generate sessionId. Endpoint %s %s",
        "GET /passport/v1/sso/getId/{nonce}", e.getMessage()));
      responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getIdJson("ID not found for nonce", null));
    }
  }
  @Override
  public void init(HandlerContext context) {
  }

  @Override
  public void destroy(HandlerContext context) {
  }
}

