package com.continuuity.passport.http.handlers;

import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.passport.core.exceptions.OrganizationAlreadyExistsException;
import com.continuuity.passport.core.exceptions.OrganizationNotFoundException;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.meta.Organization;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.io.IOUtils;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

/**
 * Handler for Organization CRUD operation.
 */

@Path("/passport/v1/organizations/")
@Singleton
public class OrganizationHandler extends PassportHandler implements HttpHandler {
  private final DataManagementService dataManagementService;
  private static final Logger LOG = LoggerFactory.getLogger(AccountHandler.class);

  @Inject
  public OrganizationHandler(DataManagementService dataManagementService) {
    this.dataManagementService = dataManagementService;
  }

  @POST
  @Produces("application/json")
  @Consumes("application/json")
  public void createOrganization(HttpRequest request, HttpResponder responder) {
    requestReceived();
    try {
      String data = IOUtils.toString(new ChannelBufferInputStream(request.getContent()));

      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      String id = jsonObject.get("id") == null ? null : jsonObject.get("id").getAsString();
      String name = jsonObject.get("name") == null ? null : jsonObject.get("name").getAsString();

      if (id == null || name == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                            Utils.getJson("FAILED", "Id and/or name is missing"));
      } else {
        Organization org = this.dataManagementService.createOrganization(id, name);
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, org.toString());
      }
    } catch (OrganizationAlreadyExistsException e) {
      requestFailed();
      responder.sendString(HttpResponseStatus.CONFLICT,
                           Utils.getJsonError("FAILED", "Organization already exists"));
    } catch (JsonParseException e) {
      requestFailed();
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           Utils.getJson("FAILED", String.format("Json parse exception. %s", e.getMessage())));
    } catch (Throwable e) {
      requestFailed();
      LOG.error("Internal server error while processing endpoint: POST /passport/v1/organizations {}", e.getMessage());
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", String.format("Organization Creation Failed. %s", e)));
    }
  }


  @PUT
  @Path("{id}")
  public void updateOrganization(HttpRequest request, HttpResponder responder,
                                 @PathParam("id") String id) {
    requestReceived();
    try {
      String data = IOUtils.toString(new ChannelBufferInputStream(request.getContent()));

      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      String name = jsonObject.get("name") == null ? null : jsonObject.get("name").getAsString();

      if (name == null) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             Utils.getJson("FAILED", "Name is missing"));
      } else {
        Organization org = this.dataManagementService.updateOrganization(id, name);
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, org.toString());
      }
    } catch (JsonParseException e) {
      requestFailed();
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           Utils.getJson("FAILED", String.format("Json parse exception. %s", e.getMessage())));
    } catch (OrganizationNotFoundException e) {
      requestFailed();
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           Utils.getJsonError("FAILED", "Organization already exists"));
    }  catch (Exception e) {
      requestFailed();
      LOG.error("Internal server error while processing endpoint: PUT /passport/v1/organization/{} {}",
                id, e.getMessage());
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", String.format("Organization update Failed. %s", e)));
    }
  }

  @DELETE
  @Path("{id}")
  public void deleteOrganization(HttpRequest request, HttpResponder responder,
                                 @PathParam("id") String id) {
    requestReceived();
    try {
      this.dataManagementService.deleteOrganization(id);
      requestSuccess();
      responder.sendString(HttpResponseStatus.OK, "Delete Successful");
    } catch (OrganizationNotFoundException e) {
      requestFailed();
      responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getJsonError("FAILED", "Organization already exists"));
    }  catch (Exception e) {
      requestFailed();
      LOG.error("Internal server error while processing endpoint: DELETE /passport/v1/organizations/{} {}",
                id, e.getMessage());
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", String.format("Organization delete Failed. %s", e)));
    }
  }

  @GET
  @Path("{id}")
  public void getOrganization(HttpRequest request, HttpResponder responder,
                              @PathParam("id") String id) {
    requestReceived();
    try {
      Organization org = this.dataManagementService.getOrganization(id);
      requestSuccess();
      responder.sendString(HttpResponseStatus.OK, org.toString());
    } catch (OrganizationNotFoundException e) {
      responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getJsonError("Organization not found"));
    } catch (Exception e) {
      requestFailed();
      LOG.error("Internal server error while processing endpoint: GET /passport/v1/organizations/{} {}",
                id, e.getMessage());
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", String.format("Organization get Failed. %s", e)));
    }
  }
  @Override
  public void init(HandlerContext context) {
  }

  @Override
  public void destroy(HandlerContext context) {
  }
}
