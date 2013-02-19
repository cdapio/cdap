package com.continuuity.passport.http.handlers;

import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.http.server.Utils;
import com.google.inject.Inject;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 *
 */
@Path("passport/v1/sso/")
public class NonceHandler {

  private final DataManagementService dataManagementService;

  @Inject
  public NonceHandler(DataManagementService dataManagementService) {
    this.dataManagementService = dataManagementService;
  }


  @Path("getNonce/{id}")
  @GET
  @Produces("application/json")
  public Response getSessionNonce(@PathParam("id") int id){
    try {
      int nonce = dataManagementService.getSessionNonce(id);
      if (nonce != -1){
        return Response.ok(Utils.getNonceJson(nonce)).build();
      }
      else {
        return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
          .entity(Utils.getNonceJson("Couldn't generate nonce", id))
          .build();
      }
    } catch (StaleNonceException e) {
      return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
        .entity(Utils.getNonceJson("Couldn't generate nonce",id))
        .build();
    }
  }

  @Path("getId/{nonce}")
  @GET
  @Produces("application/json")
  public Response getSessionId(@PathParam("nonce") int nonce){
    try {
      int id = dataManagementService.getSessionId(nonce);
      if (id != -1){
        return Response.ok(Utils.getNonceJson(id)).build();
      }
      else {
        return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
          .entity(Utils.getNonceJson("ID not found for nonce", nonce))
          .build();
      }
    } catch (StaleNonceException e) {
      return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
        .entity(Utils.getNonceJson("ID not found for nonce",nonce))
        .build();
    }
  }


}

