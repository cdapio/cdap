/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.handlers;

import com.continuuity.passport.PassportConstants;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.meta.VPC;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Defines End point for vpc
 */

@Path("passport/v1/vpc")
@Singleton
public class VPCHandler  extends PassportHandler {

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
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJsonError("VPC get Failed", e))
        .build();
    }
  }

}
