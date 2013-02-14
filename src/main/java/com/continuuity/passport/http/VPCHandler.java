package com.continuuity.passport.http;

import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.meta.VPC;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.dal.db.Common;
import com.continuuity.passport.impl.DataManagementServiceImpl;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.shiro.util.StringUtils;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;

/**
 *
 */

@Path("/passport/v1/vpc/")
public class VPCHandler  {

  @Path("/create")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public Response createVPC(String data)  {

    try {
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      JsonElement name = jsonObject.get("vpc_name");
      JsonElement id = jsonObject.get("account_id");

      String vpcName = StringUtils.EMPTY_STRING;
      int accountid = -1;

      if (id != null) {
        accountid = id.getAsInt();
      }
      if ( name != null)  {
        vpcName = name.getAsString();
      }

      if ( (accountid != -1) && (!vpcName.isEmpty()) ){
        DataManagementServiceImpl.getInstance().addVPC(accountid, new VPC(vpcName));
        return Response.ok(Utils.getJson("OK","VPC Created")).build();
      }
      else {
        return Response.status(Response.Status.BAD_REQUEST)
                          .entity(Utils.getJson("FAILED","VPC creation failed. account_id or vpc_name is missing"))
                          .build();
      }
    }
    catch (Exception e ){
      return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Utils.getJson("FAILED","VPC Creation Failed",e))
                        .build();

    }
  }
}
