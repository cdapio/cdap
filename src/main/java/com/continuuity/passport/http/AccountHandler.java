package com.continuuity.passport.http;

import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.meta.AccountSecurity;
import com.continuuity.passport.core.meta.UsernamePasswordApiKeyCredentials;
import com.continuuity.passport.core.meta.VPC;
import com.continuuity.passport.core.status.AuthenticationStatus;
import com.continuuity.passport.impl.AuthenticatorImpl;
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
 * Annotations for endpoints, method types and data types for handling Http requests
 * Note: Jersey has a limitation of not allowing multiple resource handlers share the same path.
 *       As a result we are needing to have all the code in a single file. This will be potentially
 *       huge. Need to find a work-around.
 */


@Path("/passport/v1/account/")
public class AccountHandler {
  @Path("{id}")
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



  @Path("create")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public Response createAccount(String data) {

    try{
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      String firstName = jsonObject.get("first_name") == null? null : jsonObject.get("first_name").getAsString();
      String lastName = jsonObject.get("first_name") == null? null : jsonObject.get("last_name").getAsString();
      String emailId = jsonObject.get("email_id") == null? null : jsonObject.get("email_id").getAsString();
      String company = jsonObject.get("company") == null? null : jsonObject.get("company").getAsString();

      if ( (firstName == null) || (lastName == null) || (emailId == null) || (company == null) ){
        return Response.status(Response.Status.BAD_REQUEST)
          .entity(Utils.getJson("FAILED", "First/last name or email id or company is missing")).build();
      }
      else {
        long genId = DataManagementServiceImpl.getInstance().registerAccount(new Account(firstName,
                                                                                         lastName,company,emailId));
        return Response.ok(Utils.getJson("OK","Account Created",genId)).build();
      }
    }
    catch (Exception e){
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJson("FAILED", "Account Creation Failed", e))
        .build();
    }
  }

  @Path("{id}/confirm")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public Response confirmAccount(String data, @PathParam("id") int id){
    try{
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      JsonElement password = jsonObject.get("password");

      String accountPassword = StringUtils.EMPTY_STRING;


      if(password !=null){
        accountPassword = password.getAsString();
      }

      if ( accountPassword.isEmpty()){
        return Response.status(Response.Status.BAD_REQUEST)
          .entity(Utils.getJson("FAILED","Password is missing")).build();
      }
      else {

        AccountSecurity security = new AccountSecurity(id, accountPassword);
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


  @Path("{id}/vpc/create")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public Response createVPC(String data, @PathParam("id")int id)  {

    try {
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      JsonElement name = jsonObject.get("vpc_name");

      String vpcName = StringUtils.EMPTY_STRING;

      if ( name != null)  {
        vpcName = name.getAsString();
      }

      if ( (!vpcName.isEmpty()) ){
        long genId = DataManagementServiceImpl.getInstance().addVPC(id, new VPC(vpcName));
        return Response.ok(Utils.getJson("OK","VPC Created",genId)).build();
      }
      else {
        return Response.status(Response.Status.BAD_REQUEST)
          .entity(Utils.getJson("FAILED", "VPC creation failed. vpc_name is missing"))
          .build();
      }
    }
    catch (Exception e ){
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJson("FAILED", "VPC Creation Failed", e))
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

  @Path("authenticate")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public Response authenticate(String data){

    JsonParser parser = new JsonParser();
    JsonElement element = parser.parse(data);
    JsonObject jsonObject = element.getAsJsonObject();

    String password = jsonObject.get("password") == null? null : jsonObject.get("password").getAsString();
    String emailId = jsonObject.get("email_id") == null? null : jsonObject.get("email_id").getAsString();


    try {
      AuthenticationStatus status = AuthenticatorImpl.getInstance()
                                     .authenticate(new UsernamePasswordApiKeyCredentials(emailId, password,
                                                                                         StringUtils.EMPTY_STRING));
      if (status.getType().equals(AuthenticationStatus.Type.AUTHENTICATED)) {
        //TODO: Better naming for authenticatedJson?
        return Response.ok(Utils.getAuthenticatedJson("OK",status.getMessage())).build();
      }
      else {
        return Response.status(Response.Status.UNAUTHORIZED).entity(
          Utils.getJson("FAILED","Authentication Failed. Either user doesn't exist or password doesn't match")).build();
      }
    } catch (Exception e) {

      return    Response.status(Response.Status.UNAUTHORIZED).entity(
        Utils.getJson("FAILED","Authentication Failed",e)).build();
    }

  }

}
