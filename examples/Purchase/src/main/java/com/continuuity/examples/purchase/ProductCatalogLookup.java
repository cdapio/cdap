package com.continuuity.examples.purchase;

import com.continuuity.http.AbstractHttpHandler;
import com.continuuity.http.HttpResponder;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Lookup Handler to handle users interest HTTP call.
 */
@Path("/v1")
public final class ProductCatalogLookup extends AbstractHttpHandler {

  @Path("product/{id}/catalog")
  @GET
  public void handler(HttpRequest request, HttpResponder responder, @PathParam("id") String id) {
    responder.sendString(HttpResponseStatus.OK, "Cat-" + id);
  }

}
