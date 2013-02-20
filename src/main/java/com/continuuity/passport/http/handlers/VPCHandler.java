package com.continuuity.passport.http.handlers;

import com.continuuity.passport.core.meta.VPC;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.http.server.Utils;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 *
 */

@Path("passport/v1/vpc")
@Singleton
public class VPCHandler {

  private final DataManagementService dataManagementService;

  @Inject
  public VPCHandler(DataManagementService dataManagementService) {
    this.dataManagementService = dataManagementService;
  }

  @GET
  @Produces("application/json")
  public Response getVPC(@HeaderParam("X-Continuuity-ApiKey") String apiKey) {
    try {
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
        return Response.ok(returnJson.toString()).build();
      }
    } catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJsonError("VPC get Failed", e))
        .build();
    }
  }

}
