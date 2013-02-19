package com.continuuity.passport.http.handlers;


import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.http.server.Utils;
import com.continuuity.passport.impl.DataManagementServiceImpl;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;

/**
 *
 */
@Path("/passport/v1")
public class ActivationHandler {

  @Path("generateActivationKey/{id}")
  @GET
  @Produces("application/json")
  public Response getActivationNonce(@PathParam("id") int id){
    try {
      int nonce = DataManagementServiceImpl.getInstance().getActivationNonce(id);
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
      int nonce = DataManagementServiceImpl.getInstance().getActivationId(id);
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
