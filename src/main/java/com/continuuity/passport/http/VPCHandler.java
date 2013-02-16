package com.continuuity.passport.http;

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
      Gson gson = new Gson();
      if (vpcList.isEmpty()) {
        return Response.ok("[]").build();
      }
      else {
        return Response.ok(gson.toJson(vpcList)).build();
      }
    }
    catch(Exception e){
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJson("FAILED", "VPC get Failed", e))
        .build();
    }
  }

}
