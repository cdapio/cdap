package com.continuuity.passport.http.handlers;


import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.http.server.Utils;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 *
 */
@Path("/passport/v1")
@Singleton
public class ActivationHandler {

  private final DataManagementService dataManagementService;

  @Inject
  public ActivationHandler(DataManagementService dataManagementService) {
    this.dataManagementService = dataManagementService;
  }

  @Path("generateActivationKey/{id}")
  @GET
  @Produces("application/json")
  public Response getActivationNonce(@PathParam("id") int id){
    try {
      int nonce = dataManagementService.getActivationNonce(id);
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

  @Path("getActivationId/{id}")
  @GET
  @Produces("application/json")
  public Response getActivationId(@PathParam("id") int id){
    try {
      int nonce = dataManagementService.getActivationId(id);
      if (nonce != -1){
        return Response.ok(Utils.getNonceJson(nonce)).build();
      }
      else {
        return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
          .entity(Utils.getNonceJson("ID not found for nonce", id))
          .build();
      }
    } catch (StaleNonceException e) {
      return Response.status(javax.ws.rs.core.Response.Status.NOT_FOUND)
        .entity(Utils.getNonceJson("ID not found for nonce",id))
        .build();
    }
  }


}
