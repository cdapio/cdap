package com.continuuity.passport.http.handlers;

import com.continuuity.passport.core.exceptions.AccountAlreadyExistsException;
import com.continuuity.passport.core.exceptions.AccountNotFoundException;
import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.meta.AccountSecurity;
import com.continuuity.passport.core.meta.UsernamePasswordApiKeyCredentials;
import com.continuuity.passport.core.meta.VPC;
import com.continuuity.passport.core.service.Authenticator;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.core.status.AuthenticationStatus;
import com.continuuity.passport.http.server.Utils;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.shiro.util.StringUtils;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Annotations for endpoints, method types and data types for handling Http requests
 * Note: Jersey has a limitation of not allowing multiple resource handlers share the same path.
 *       As a result we are needing to have all the code in a single file. This will be potentially
 *       huge. Need to find a work-around.
 */


@Path("/passport/v1/account/")
@Singleton
public class AccountHandler {

  private final DataManagementService dataManagementService;
  private final Authenticator authenticator;


  @Inject
  public AccountHandler(DataManagementService dataManagementService, Authenticator authenticator) {
    this.dataManagementService = dataManagementService;
    this.authenticator = authenticator;
  }

  @Path("{id}")
  @GET
  @Produces("application/json")
  public Response getAccountInfo(@PathParam("id") int id){

    Account account = dataManagementService.getAccount(id);
    if (account != null){
      return Response.ok(account.toString()).build();
    }
    else {
      return Response.status(Response.Status.NOT_FOUND)
        .entity(Utils.getJsonError("Account not found"))
        .build();
    }
  }



  @Path("{id}/password")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public Response changePassword(@PathParam("id") int id, String data){

    try {
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      String oldPassword = jsonObject.get("old_password") == null? null : jsonObject.get("old_password").getAsString();
      String newPassword = jsonObject.get("new_password") == null? null : jsonObject.get("new_password").getAsString();

      if ( (oldPassword == null ) || (oldPassword.isEmpty()) ||
        (newPassword == null) || (newPassword.isEmpty()) ) {
        return Response.status(Response.Status.BAD_REQUEST)
          .entity(Utils.getJson("FAILED", "Must pass in old_password and new_password"))
          .build();
      }

      dataManagementService.changePassword(id, oldPassword, newPassword);
      //Contract for the api is to return updated account to avoid a second call from the caller to get the
      // updated account
      Account account = dataManagementService.getAccount(id);
      if ( account !=null) {
        return Response.ok(account.toString()).build();
      }
      else {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(Utils.getJson("FAILED", "Failed to get updated account"))
          .build();
      }
    }
    catch (Exception e){
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJson("FAILED","Download confirmation failed",e))
        .build();
    }
  }

  @Path("{id}/downloaded")
  @PUT
  @Produces("application/json")
  public Response confirmDownload(@PathParam("id") int id){

    try {

      dataManagementService.confirmDownload(id);
      //Contract for the api is to return updated account to avoid a second call from the caller to get the
      // updated account
      Account account = dataManagementService.getAccount(id);
      if ( account !=null) {
        return Response.ok(account.toString()).build();
      }
      else {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(Utils.getJson("FAILED", "Failed to get updated account"))
          .build();
      }
    }
    catch (Exception e){
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJson("FAILED","Download confirmation failed",e))
        .build();
    }
  }

  @Path ("{id}")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public Response updateAccount(@PathParam("id")int id, String data){

    try {
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      Map<String,Object> updateParams = new HashMap<String,Object>();

      String firstName = jsonObject.get("first_name") == null? null : jsonObject.get("first_name").getAsString();
      String lastName = jsonObject.get("last_name") == null? null : jsonObject.get("last_name").getAsString();
      String company = jsonObject.get("company") == null? null : jsonObject.get("company").getAsString();

      //TODO: Find a better way to update the map
      if ( firstName != null ) {
        updateParams.put("first_name",firstName);
      }

      if ( lastName != null ) {
        updateParams.put("last_name",lastName);
      }

      if ( company != null ) {
        updateParams.put("company",company);
      }

      dataManagementService.updateAccount(id, updateParams);
      //Contract for the api is to return updated account to avoid a second call from the caller to get the
      // updated account
      Account account = dataManagementService.getAccount(id);
      if ( account !=null) {
        return Response.ok(account.toString()).build();
      }
      else {
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
          .entity(Utils.getJson("FAILED", "Failed to get updated account"))
          .build();
      }
    }
    catch(Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJson("FAILED", "Account Update Failed", e))
        .build();
    }
  }

  @POST
  @Produces("application/json")
  @Consumes("application/json")
  public Response createAccount(String data) {
    String emailId = null;
    try{
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

       emailId = jsonObject.get("email_id") == null? null : jsonObject.get("email_id").getAsString();

      if (  (emailId == null)  ){
        return Response.status(Response.Status.BAD_REQUEST)
          .entity(Utils.getJson("FAILED", "Email id is missing")).build();
      }
      else {
        Account account = dataManagementService.registerAccount(new Account("", "", "", emailId));
        return Response.ok(account.toString()).build();
      }
    } catch (AccountAlreadyExistsException e) {
      //If the account already exists - return the existing account so that the caller can take appropriate action
      Account account = dataManagementService.getAccount(emailId);
      return Response.status(Response.Status.CONFLICT)
        .entity(Utils.getJsonError("FAILED", account.toString()))
        .build();
    }
    catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJson("FAILED", "Account Creation Failed", e))
        .build();
    }
  }

  @Path("{id}/confirmed")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public Response confirmAccount(String data, @PathParam("id") int id){
    try{
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      String accountPassword = jsonObject.get("password") == null? null : jsonObject.get("password").getAsString();
      String firstName = jsonObject.get("first_name") == null? null : jsonObject.get("first_name").getAsString();
      String lastName = jsonObject.get("last_name") == null? null : jsonObject.get("last_name").getAsString();
      String company = jsonObject.get("company") == null? null : jsonObject.get("company").getAsString();


      if ( (accountPassword == null) ||  (accountPassword.isEmpty()) ||
        (firstName == null) ||  (firstName.isEmpty()) ||
        (lastName == null) ||  (lastName.isEmpty()) ||
        (company == null) ||  (company.isEmpty())) {
        return Response.status(Response.Status.BAD_REQUEST)
          .entity(Utils.getJson("FAILED","password, first_name, last_name, company should be passed in")).build();
      }
      else {
        Account account = new Account(firstName, lastName,company,id);
        AccountSecurity security = new AccountSecurity(account, accountPassword);
        dataManagementService.confirmRegistration(account, accountPassword);
          //Contract for the api is to return updated account to avoid a second call from the caller to get the
        // updated account
        Account accountFetched = dataManagementService.getAccount(id);
        if ( accountFetched !=null) {
          return Response.ok(accountFetched.toString()).build();
        }
        else {
          return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
            .entity(Utils.getJson("FAILED", "Failed to get updated account"))
            .build();
        }
      }
    }
    catch (Exception e){
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJson("FAILED","Account Confirmation Failed",e))
        .build();
    }
  }


  @Path("{id}/vpc")
  @POST
  @Produces("application/json")
  @Consumes("application/json")
  public Response createVPC(String data, @PathParam("id")int id)  {

    try {
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      String vpcName  = jsonObject.get("vpc_name") == null ? null : jsonObject.get("vpc_name").getAsString();
      String vpcLabel = jsonObject.get("vpc_label") == null ? null : jsonObject.get("vpc_label").getAsString();

      if ( (vpcName!= null) && (!vpcName.isEmpty()) && (vpcLabel!=null) && ( !vpcLabel.isEmpty()) ){
        VPC vpc= dataManagementService.addVPC(id, new VPC(vpcName, vpcLabel));
        return Response.ok(vpc.toString()).build();
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
      List<VPC> vpcList = dataManagementService.getVPC(id);
      if (vpcList.isEmpty()) {
        return Response.ok("[]").build();
      }
      else {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for (VPC vpc : vpcList) {
          if (first) {
            first = false;
          }
          else {
            sb.append(",");
          }
          sb.append(vpc.toString());
        }
        sb.append("]");
        return Response.ok(sb.toString()).build();
      }
    }
    catch(Exception e){
      return Response.status(Response.Status.BAD_REQUEST)
        .entity(Utils.getJsonError("VPC get Failed", e))
        .build();
    }
  }

  @Path("{accountId}/vpc/{vpcId}")
  @GET
  @Produces("application/json")
  public Response getSingleVPC(@PathParam("accountId") int accountId, @PathParam("vpcId") int vpcId) {

    try{
      VPC vpc = dataManagementService.getVPC(accountId,vpcId);
      if (vpc==null) {
        return Response.status(Response.Status.NOT_FOUND)
          .entity(Utils.getJsonError("VPC not found")).build();

      }
      else {
        return Response.ok(vpc.toString()).build();
      }
    }
    catch(Exception e){
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJsonError("VPC get Failed", e))
        .build();
    }
  }

  @Path("authenticate")
  @POST
  @Produces("application/json")
  @Consumes("application/json")
  public Response authenticate(String data){

    JsonParser parser = new JsonParser();
    JsonElement element = parser.parse(data);
    JsonObject jsonObject = element.getAsJsonObject();

    String password = jsonObject.get("password") == null? null : jsonObject.get("password").getAsString();
    String emailId = jsonObject.get("email_id") == null? null : jsonObject.get("email_id").getAsString();


    try {

      AuthenticationStatus status = authenticator.authenticate(new UsernamePasswordApiKeyCredentials(emailId, password,
        StringUtils.EMPTY_STRING));
      if (status.getType().equals(AuthenticationStatus.Type.AUTHENTICATED)) {
        //TODO: Better naming for authenticatedJson?
        return Response.ok(Utils.getAuthenticatedJson(status.getMessage())).build();
      }
      else {
        return Response.status(Response.Status.UNAUTHORIZED).entity(
          Utils.getAuthenticatedJson("Authentication Failed." , "Either user doesn't exist or password doesn't match"))
          .build();
      }
    } catch (Exception e) {

      return    Response.status(Response.Status.UNAUTHORIZED).entity(
        Utils.getAuthenticatedJson("Authentication Failed.",e.getMessage())).build();
    }
  }

  @Path("{id}")
  @DELETE
  @Produces("application/json")
  public Response deleteAccount(@PathParam("id") int id){

    try {

      dataManagementService.deleteAccount(id);
      return Response.ok().entity(Utils.getJsonOK()).build();
    } catch (AccountNotFoundException e) {
      return Response.status(Response.Status.NOT_FOUND)
        .entity(Utils.getJsonError("Account not found"))
        .build();
    }
    catch(RuntimeException e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJsonError("Account delete Failed", e.getMessage()))
        .build();

    }
  }

  @Path("{accountId}/vpc/{vpcId}")
  @DELETE
  @Produces("application/json")
  public Response deleteVPC(@PathParam("accountId") int accountId,  @PathParam("vpcId") int vpcId){

    try {
      dataManagementService.deleteVPC(accountId, vpcId);
      return Response.ok().entity(Utils.getJsonOK()).build();
    } catch (VPCNotFoundException e) {
      return Response.status(Response.Status.NOT_FOUND)
        .entity(Utils.getJsonError("VPC not found"))
        .build();
    }
    catch(RuntimeException e) {
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
        .entity(Utils.getJsonError("VPC delete Failed",e.getMessage()))
        .build();

    }
  }

}
