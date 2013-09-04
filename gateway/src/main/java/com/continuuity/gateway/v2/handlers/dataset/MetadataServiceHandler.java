package com.continuuity.gateway.v2.handlers.dataset;

import com.continuuity.common.http.core.AbstractHttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.gateway.auth.GatewayAuthenticator;
import com.continuuity.gateway.v2.handlers.AuthenticatedHttpHandler;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.continuuity.metadata.thrift.Stream;
import com.google.inject.Inject;
import org.apache.thrift.TException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;

/**
 *
 */
@Path("/v2")
public class MetadataServiceHandler extends AuthenticatedHttpHandler {
  private final MetadataService service;

  @Inject
  public MetadataServiceHandler(MetadataService service, GatewayAuthenticator authenticator) {
    super(authenticator);
    this.service = service;
  }

  @GET
  @Path("/streams")
  public void getStreams(HttpRequest request, HttpResponder responder) {
    try {
      String accountId = getAuthenticatedAccountId(request);
      List<Stream> streams = service.getStreams(new Account(accountId));
      JSONArray s = new JSONArray();
      for (Stream stream : streams) {
        JSONObject object = new JSONObject();
        object.append("id", stream.getId());
        object.append("name", stream.getName());
        object.append("description", stream.getDescription());
        object.append("expiry", stream.getExpiryInSeconds());
        object.append("capacity", stream.getCapacityInBytes());
        s.put(object);
      }
      responder.sendJson(HttpResponseStatus.OK, s);
    } catch (SecurityException e) {
      responder.sendStatus(HttpResponseStatus.FORBIDDEN);
    } catch (IllegalArgumentException e) {
      responder.sendStatus(HttpResponseStatus.NOT_FOUND);
    } catch (Exception e) {
      responder.sendStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @GET
  @Path("/datasets")
  public void getDatasets(HttpRequest request, HttpResponder responder) {

  }

  @GET
  @Path("/queries")
  public void getQueries(HttpRequest request, HttpResponder responder) {

  }

  @GET
  @Path("/mapreduces")
  public void getMapReduces(HttpRequest request, HttpResponder responder) {

  }

  @GET
  @Path("/apps")
  public void getApps(HttpRequest request, HttpResponder responder) {

  }


}
