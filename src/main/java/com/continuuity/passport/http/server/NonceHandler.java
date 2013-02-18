package com.continuuity.passport.http.server;

import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.impl.DataManagementServiceImpl;

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

  @Path("getNonce/{id}")
  @GET
  @Produces("application/json")
  public Response getNonce(@PathParam("id") int id){
    try {
      int nonce = DataManagementServiceImpl.getInstance().getNonce(id);
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

  @Path("getNonce/{nonce}")
  @GET
  @Produces("application/json")
  public Response getId(@PathParam("nonce") int nonce){
    try {
      int id = DataManagementServiceImpl.getInstance().getId(nonce);
      if (id != -1){
        return Response.ok(Utils.getNonceJson(nonce)).build();
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

