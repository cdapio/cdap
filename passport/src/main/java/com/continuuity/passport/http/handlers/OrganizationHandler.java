package com.continuuity.passport.http.handlers;

import com.continuuity.passport.core.service.AuthenticatorService;
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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
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
    try{
    JsonParser parser = new JsonParser();
    JsonElement element = parser.parse(data);
    JsonObject jsonObject = element.getAsJsonObject();

    String id = jsonObject.get("id") == null ? null : jsonObject.get("id").getAsString();
    String name = jsonObject.get("name") == null ? null : jsonObject.get("name").getAsString();

    if (id == null || name == null ){
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJson("FAILED", "Id and/or name is missing")).build();
    }
      try{
        Organization org = this.dataManagementService.createOrganization(id, name);
        requestSuccess();
        return Response.ok(org.toString()).build();

      } catch (Exception e){
        requestFailed(); // Request failed
        LOG.error("Call to POST /passport/v1/organization Failed. Organization already exists");
        return Response.status(Response.Status.CONFLICT)
          .entity(Utils.getJsonError("FAILED", "Organization already exists"))
          .build();
      }
    }
   catch (JsonParseException e){
     requestFailed();
     LOG.error("Bad request while processing endpoint: POST /passport/v1/account {}", e.getMessage());
     return Response.status(Response.Status.BAD_REQUEST)
       .entity(Utils.getJson("FAILED", String.format("Json parse exception. %s", e.getMessage())))
       .build();
   } catch (Exception e) {
      requestFailed();
      LOG.error(String.format("Internal server error while processing endpoint: %s %s",
                              "POST /passport/v1/organization", e.getMessage()));
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJson("FAILED", String.format("Organization Creation Failed. %s", e)))
        .build();
    }
  }
}
