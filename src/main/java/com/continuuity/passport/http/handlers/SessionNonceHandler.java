/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.handlers;

import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.core.service.DataManagementService;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 * Handle Nonce operation for Session nonce
 */
@Path("passport/v1/sso/")
@Singleton
public class SessionNonceHandler extends PassportHandler {

  private final DataManagementService dataManagementService;

  @Inject
  public SessionNonceHandler(DataManagementService dataManagementService) {
    this.dataManagementService = dataManagementService;
  }


  @Path("getNonce/{id}")
  @GET
  @Produces("application/json")
  public Response getSessionNonce(@PathParam("id") int id) {
    requestReceived();
    try {
      int nonce = dataManagementService.getSessionNonce(id);
      if (nonce != -1) {
        requestSuccess();
        return Response.ok(Utils.getNonceJson(nonce)).build();
      } else {
        requestFailed();
        return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
          .entity(Utils.getNonceJson("Couldn't generate nonce", id))
          .build();
      }
    } catch (RuntimeException e) {
      requestFailed();
      return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
        .entity(Utils.getNonceJson("Couldn't generate nonce", id))
        .build();
    }
  }

  @Path("getId/{nonce}")
  @GET
  @Produces("application/json")
  public Response getSessionId(@PathParam("nonce") int nonce) {
    requestReceived();
    try {
      int id = dataManagementService.getSessionId(nonce);
      if (id != -1) {
        requestSuccess();
        return Response.ok(Utils.getNonceJson(id)).build();
      } else {
        requestFailed();
        return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
          .entity(Utils.getNonceJson("ID not found for nonce", nonce))
          .build();
      }
    } catch (StaleNonceException e) {
      requestFailed();
      return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
        .entity(Utils.getNonceJson("ID not found for nonce", nonce))
        .build();
    }
  }


}

