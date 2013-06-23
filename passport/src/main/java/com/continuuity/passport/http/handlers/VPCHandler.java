/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.handlers;

import com.continuuity.passport.PassportConstants;
import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.RolesAccounts;
import com.continuuity.passport.meta.VPC;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * Defines End point for vpc related functions.
 */

@Path("passport/v1/vpc")
@Singleton
public class VPCHandler extends PassportHandler {
  private static final Logger LOG = LoggerFactory.getLogger(VPCHandler.class);
  private final DataManagementService dataManagementService;

  @Inject
  public VPCHandler(DataManagementService dataManagementService) {
    this.dataManagementService = dataManagementService;
  }

  @GET
  @Produces("application/json")
  public Response getVPC(@HeaderParam(PassportConstants.CONTINUUITY_API_KEY_HEADER) String apiKey) {
    try {
      requestReceived();
      List<VPC> vpcList = dataManagementService.getVPC(apiKey);
      if (vpcList.isEmpty()) {
        return Response.ok("[]").build();
      } else {
        StringBuilder returnJson = new StringBuilder();
        returnJson.append("[");
        boolean first = true;
        for (VPC vpc : vpcList) {
          if (first) {
            first = false;
          } else {
            returnJson.append(",");
          }
          returnJson.append(vpc.toString());
        }
        returnJson.append("]");
        requestSuccess();
        return Response.ok(returnJson.toString()).build();
      }
    } catch (Exception e) {
      requestFailed();
      LOG.error(String.format("Internal server error processing endpoint: %s %s",
        "GET /passport/v1/vpc}", e.getMessage()));
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJsonError(String.format("VPC get Failed. %s", e)))
        .build();
    }
  }

  @Path("valid/{vpcName}")
  @GET
  public Response isValidVPC(@PathParam("vpcName") String vpcName) {
    try {
      if (dataManagementService.isValidVPC(vpcName)) {
        return Response.ok().entity(Utils.getJsonOK()).build();
      } else {
        return Response.ok().entity(Utils.getJsonError("VPC already exists")).build();
      }
    } catch (Exception e) {
      requestFailed();
      LOG.error(String.format("Internal server error processing endpoint: %s %s",
                              "GET /passport/v1/vpc/valid/{vpcName}", e.getMessage()));
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJsonError("FAILED", e.getMessage()))
        .build();
    }

  }

  @Path("{vpcName}")
  @GET
  public Response getAccountForVPCName(@PathParam("vpcName") String vpcName) {
    try {
      Account account = dataManagementService.getAccountForVPC(vpcName);
      if (account != null) {
        requestSuccess();
        return Response.ok(account.toString()).build();
      } else {
        requestFailed();
        LOG.error(String.format("Account not found. Processing endpoint: %s ", "GET /passport/v1/vpc/{vpcName}"));
        return Response.status(Response.Status.NOT_FOUND)
          .entity(Utils.getJsonError("Account not found for VPC"))
          .build();
      }
    } catch (Exception e) {
      requestFailed();
      LOG.error(String.format("Account not found. endpoint: %s %s", "GET /passport/v1/vpc/{vpcName}", e.getMessage()));
      return Response.status(Response.Status.NOT_FOUND)
        .entity(Utils.getJsonError("Account not found for VPC"))
        .build();
    }
  }

  /**
   * Gets account if for VPC.
   * Endpoint is obfuscated on purpose
   * @param vpcName VpcName
   * @return Instance of {@code Response}
   */
  @Path("xkcd/{vpcName}")
  @GET
  public Response getIdForVPC(@PathParam("vpcName") String vpcName) {
    try {
      Account account = dataManagementService.getAccountForVPC(vpcName);
      if (account != null) {
        requestSuccess();
        return Response.ok().entity(Utils.getIdJson(null, account.getAccountId())).build();
      } else {
        requestFailed();
        LOG.error(String.format("xkcd not found. Processing endpoint: %s ",
                                "GET /passport/v1/vpc/xkcd/{vpcName}"));
        return Response.status(Response.Status.NOT_FOUND)
          .entity(Utils.getIdJson("FAILED", "xkcd not found for VPC"))
          .build();
      }
    } catch (Exception e) {
      requestFailed();
      LOG.error(String.format("xkcd not found. endpoint: %s %s", "GET /passport/v1/xkcd/{vpcName}", e.getMessage()));
      return Response.status(Response.Status.NOT_FOUND)
        .entity(Utils.getIdJson("FAILED", "xkcd not found for VPC"))
        .build();
    }
  }

  @GET
  @Path("{vpcName}/accountRoles")
  public Response getAccountRoles(@PathParam("vpcName") String vpcName) {
    requestReceived();
    JsonArray accountRoleArray = new JsonArray();
    try {
      RolesAccounts rolesAccounts = dataManagementService.getAccountRoles(vpcName);
      for (String role : rolesAccounts.getRoles()) {
        JsonObject entry = new JsonObject();
        JsonArray accountArray = new JsonArray();
        for (Account account : rolesAccounts.getAccounts(role)){
          accountArray.add(account.toJson());
        }
        entry.addProperty("role", role);
        entry.add("accounts", accountArray);
        accountRoleArray.add(entry);
      }
      requestSuccess();
      return Response.ok().entity(accountRoleArray.toString()).build();
    } catch (Exception e) {
      requestFailed();
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJsonError("VPC delete Failed", e.getMessage()))
        .build();
    }
   }

  @Path("{vpcName}")
  @DELETE
  public Response deleteVPCByName(@PathParam("vpcName") String vpcName) {
    try {
      requestSuccess();
      dataManagementService.deleteVPC(vpcName);
      return Response.ok().entity(Utils.getJsonOK()).build();
    } catch (VPCNotFoundException e) {
      requestFailed(); //Failed request
      LOG.debug(String.format("VPC not found endpoint: %s %s",
        "DELETE /passport/v1/vpc/{vpcName}", e.getMessage()));
      return Response.status(Response.Status.NOT_FOUND)
        .entity(Utils.getJsonError("VPC not found"))
        .build();
    } catch (RuntimeException e) {
      requestFailed(); //Failed request
      LOG.error(String.format("Internal server error endpoint: %s %s",
        "DELETE /passport/v1/{vpcName}", e.getMessage()));
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJsonError("VPC delete Failed", e.getMessage()))
        .build();
    }
  }
}
