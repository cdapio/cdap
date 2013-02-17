package com.continuuity.passport.http.server;

import com.continuuity.passport.core.meta.VPC;
import com.continuuity.passport.impl.DataManagementServiceImpl;
import com.google.gson.Gson;

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
public class VPCHandler {

  @GET
  @Produces("application/json")
  public Response getVPC(@HeaderParam("X-Continuuity-ApiKey") String apiKey){
    try{
      List<VPC> vpcList = DataManagementServiceImpl.getInstance().getVPC(apiKey);
      if (vpcList.isEmpty()) {
        return Response.ok("[]").build();
      }
      else {
        StringBuilder returnJson = new StringBuilder();
        returnJson.append("[");
        boolean first = true;
        for(VPC vpc : vpcList) {
          if (first) {
            first= false;
          }
          else {
            returnJson.append(",");
          }
          returnJson.append(vpc.toString());

        }
        returnJson.append("]");
        return Response.ok(returnJson.toString()).build();
      }
    }
    catch(Exception e){
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJson("FAILED", "VPC get Failed", e))
        .build();
    }
  }

}
