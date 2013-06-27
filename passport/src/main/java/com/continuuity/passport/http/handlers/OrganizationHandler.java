package com.continuuity.passport.http.handlers;

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
import javax.ws.rs.core.Response;

/**
 * Handler for Organization CRUD operation.
 */

@Path("/passport/v1/organization/")
@Singleton
public class OrganizationHandler extends PassportHandler {
  private final DataManagementService dataManagementService;
  private static final Logger LOG = LoggerFactory.getLogger(AccountHandler.class);

  @Inject
  public OrganizationHandler(DataManagementService dataManagementService) {
    this.dataManagementService = dataManagementService;
  }

  @POST
  @Produces("application/json")
  @Consumes("application/json")
  public Response createOrganization(String data){
    requestReceived();
    try {
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      String id = jsonObject.get("id") == null ? null : jsonObject.get("id").getAsString();
      String name = jsonObject.get("name") == null ? null : jsonObject.get("name").getAsString();

      if (id == null || name == null){
        return Response.status(Response.Status.BAD_REQUEST)
          .entity(Utils.getJson("FAILED", "Id and/or name is missing")).build();
      }
      Organization org = this.dataManagementService.createOrganization(id, name);
      requestSuccess();
      return Response.ok(org.toString()).build();
    } catch (OrganizationAlreadyExistsException e){
      requestFailed();
      return Response.status(Response.Status.CONFLICT)
        .entity(Utils.getJsonError("FAILED", "Organization already exists"))
        .build();
    } catch (JsonParseException e){
      requestFailed();
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJson("FAILED", String.format("Json parse exception. %s", e.getMessage())))
        .build();
    } catch (Throwable e) {
      requestFailed();
      LOG.error(String.format("Internal server error while processing endpoint: %s %s",
                              "POST /passport/v1/organization", e.getMessage()));
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJson("FAILED", String.format("Organization Creation Failed. %s", e)))
        .build();
    }
  }


  @PUT
  @Path("{id}")
  public Response updateOrganization(@PathParam("id") String id, String data){
    requestReceived();
    try {
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      String name = jsonObject.get("name") == null ? null : jsonObject.get("name").getAsString();

      if (name == null){
        return Response.status(Response.Status.BAD_REQUEST)
          .entity(Utils.getJson("FAILED", "Name is missing")).build();
      }

      Organization org = this.dataManagementService.updateOrganization(id, name);
      requestSuccess();
      return Response.ok(org.toString()).build();
    } catch (JsonParseException e){
      requestFailed();
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJson("FAILED", String.format("Json parse exception. %s", e.getMessage())))
        .build();
    } catch (OrganizationNotFoundException e){
      requestFailed();
      return Response.status(Response.Status.NOT_FOUND)
        .entity(Utils.getJsonError("FAILED", "Organization already exists"))
        .build();
    }  catch (Exception e) {
      requestFailed();
      LOG.error("Internal server error while processing endpoint: PUT /passport/v1/organization/{} {}",
                id, e.getMessage());
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJson("FAILED", String.format("Organization update Failed. %s", e)))
        .build();
    }
  }

  @DELETE
  @Path("{id}")
  public Response deleteOrganization(@PathParam("id") String id) {
    requestReceived();
    try {
      this.dataManagementService.deleteOrganization(id);
      requestSuccess();
      return Response.ok("Delete successful").build();
    } catch (OrganizationNotFoundException e){
      requestFailed();
      return Response.status(Response.Status.NOT_FOUND)
        .entity(Utils.getJsonError("FAILED", "Organization already exists"))
        .build();
    }  catch (Exception e) {
      requestFailed();
      LOG.error("Internal server error while processing endpoint: DELETE /passport/v1/organization/{} {}",
                id, e.getMessage());
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJson("FAILED", String.format("Organization delete Failed. %s", e)))
        .build();
    }
  }

  @GET
  @Path("{id}")
  public Response updateOrganization(@PathParam("id") String id) {
    requestReceived();
    try {
      Organization org = this.dataManagementService.getOrganization(id);
      requestSuccess();
      return Response.ok(org.toString()).build();
    } catch (Exception e) {
      requestFailed();
      LOG.error("Internal server error while processing endpoint: GET /passport/v1/organization/{} {}",
                id, e.getMessage());
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJson("FAILED", String.format("Organization get Failed. %s", e)))
        .build();
    }
  }
}
