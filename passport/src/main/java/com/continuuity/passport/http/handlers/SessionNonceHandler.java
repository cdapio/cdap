/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.handlers;

import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.core.service.SecurityService;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Handle Nonce operation for Session nonce.
 */
@Path("passport/v1/sso/")
@Singleton
public class SessionNonceHandler extends PassportHandler {

  private final SecurityService securityService;
  private static final Logger LOG = LoggerFactory.getLogger(SessionNonceHandler.class);

  @Inject
  public SessionNonceHandler(SecurityService securityService) {
    this.securityService = securityService;
  }


  @Path("getNonce/{id}")
  @GET
  @Produces("application/json")
  public Response getSessionNonce(@PathParam("id") String id) {
    requestReceived();
    int nonce = -1;
    try {
      nonce = securityService.getSessionNonce(id);
      if (nonce != -1) {
        requestSuccess();
        return Response.ok(Utils.getNonceJson(null, nonce)).build();
      } else {
        requestFailed();
        LOG.error(String.format("Failed to generate nonce. Endpoint %s", "GET /passport/v1/sso/getNonce/{id}"));
        return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
          .entity(Utils.getNonceJson("Couldn't generate nonce", nonce))
          .build();
      }
    } catch (RuntimeException e) {
      requestFailed();
      LOG.error(String.format("Failed to generate nonce. Endpoint %s %s",
                              "GET /passport/v1/sso/getNonce/{id}", e.getMessage()));
      return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
        .entity(Utils.getNonceJson("Couldn't generate nonce", nonce))
        .build();
    }
  }

  @Path("getId/{nonce}")
  @GET
  @Produces("application/json")
  public Response getSessionId(@PathParam("nonce") int nonce) {
    requestReceived();
    String id = null;
    try {
      id = securityService.getSessionId(nonce);
      if ((id != null) && (!id.isEmpty())) {
        requestSuccess();
        return Response.ok(Utils.getIdJson(null, id)).build();
      } else {
        requestFailed();
        LOG.error(String.format("Failed to generate sessionId. Endpoint %s",
          "GET /passport/v1/sso/getId/{nonce}"));
        return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
          .entity(Utils.getIdJson("ID not found for nonce", null))
          .build();
      }
    } catch (StaleNonceException e) {
      requestFailed();
      LOG.error(String.format("Failed to generate sessionId. Endpoint %s %s",
        "GET /passport/v1/sso/getId/{nonce}", e.getMessage()));
      return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
        .entity(Utils.getIdJson("ID not found for nonce", null))
        .build();
    }
  }
}

