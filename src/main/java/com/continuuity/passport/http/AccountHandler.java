package com.continuuity.passport.http;

import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.meta.AccountSecurity;
import com.continuuity.passport.core.meta.VPC;
import com.continuuity.passport.impl.DataManagementServiceImpl;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.shiro.util.StringUtils;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 *
 *
 *
 */

@Path("/passport/v1/account/")
public class AccountHandler {


  @Path("{id}/status")
  @GET
  @Produces("application/json")
  public Response getAccountInfo(@PathParam("id") int id){

    Account account = DataManagementServiceImpl.getInstance().getAccount(id);
    if (account != null){
      return Response.ok(account.toString()).build();
    }
    else {
      return Response.status(Response.Status.NOT_FOUND)
        .entity(Utils.getJson("NOT_FOUND", "Account not found"))
        .build();

    }
  }

  @Path("/create")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public Response createAccount(String data) {

    try{
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      JsonElement emailId = jsonObject.get("email_id");
      JsonElement name = jsonObject.get("name");

      String accountEmail = StringUtils.EMPTY_STRING;
      String accountName = StringUtils.EMPTY_STRING;

      if (emailId != null) {
        accountEmail = emailId.getAsString();
      }
      if ( name != null)  {
        accountName = name.getAsString();
      }

      if ( (accountEmail.isEmpty()) || (accountName.isEmpty()) ){
        return Response.status(Response.Status.BAD_REQUEST)
          .entity(Utils.getJson("FAILED", "Account name or email id is missing")).build();
      }
      else {
        DataManagementServiceImpl.getInstance().registerAccount(new Account(accountName,accountEmail));
        return Response.ok(Utils.getJson("OK","Account Created")).build();
      }
    }
    catch (Exception e){
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJson("FAILED", "Account Creation Failed", e))
        .build();
    }
  }

  @Path("{id}/vpc")
  @GET
  @Produces("application/json")
  public Response getVPC(@PathParam("id") int id) {

    try{
      List<VPC> vpcList = DataManagementServiceImpl.getInstance().getVPC(id);
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

  @Path("{id}/confirm")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public Response confirmAccount(String data, @PathParam("id") String id){
//TODO: DOnot expect name and emailid in json
    try{
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      JsonElement emailId = jsonObject.get("email_id");
      JsonElement name = jsonObject.get("name");
      JsonElement password = jsonObject.get("password");

      String accountEmail = StringUtils.EMPTY_STRING;
      String accountName = StringUtils.EMPTY_STRING;
      String accountPassword = StringUtils.EMPTY_STRING;

      if (emailId != null) {
        accountEmail = emailId.getAsString();
      }
      if ( name != null)  {
        accountName = name.getAsString();
      }

      if(password !=null){
        accountPassword = password.getAsString();
      }

      if ( (accountEmail.isEmpty()) || (accountName.isEmpty()) || (accountPassword.isEmpty()) ){
        return Response.status(Response.Status.BAD_REQUEST)
          .entity(Utils.getJson("FAILED","Account name or email id or password is missing")).build();
      }
      else {

        Account account = new Account(accountName, id);
        AccountSecurity security = new AccountSecurity(account, accountPassword);
        DataManagementServiceImpl.getInstance().confirmRegistration(security);
        return Response.ok(Utils.getJson("OK","Account confirmed")).build();
      }
    }
    catch (Exception e){
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJson("FAILED","Account Confirmation Failed",e))
        .build();
    }
  }
}
