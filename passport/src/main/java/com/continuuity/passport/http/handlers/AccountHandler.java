/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.handlers;

import com.continuuity.http.HandlerContext;
import com.continuuity.http.HttpHandler;
import com.continuuity.http.HttpResponder;
import com.continuuity.passport.core.exceptions.AccountAlreadyExistsException;
import com.continuuity.passport.core.exceptions.AccountNotFoundException;
import com.continuuity.passport.core.exceptions.OrganizationNotFoundException;
import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.core.security.UsernamePasswordApiKeyToken;
import com.continuuity.passport.core.service.AuthenticatorService;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.core.status.AuthenticationStatus;
import com.continuuity.passport.core.utils.PasswordUtils;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.VPC;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.io.IOUtils;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

/**
 * Annotations for endpoints, method types and data types for handling Http requests
 * Note: Jersey has a limitation of not allowing multiple resource handlers share the same path.
 * As a result we are needing to have all the code in a single file. This will be potentially
 * huge. Need to find a work-around.
 */

@Path("/passport/v1/accounts/")
@Singleton
public class AccountHandler extends PassportHandler implements HttpHandler {

  private final DataManagementService dataManagementService;
  private final AuthenticatorService authenticatorService;
  private static final Logger LOG = LoggerFactory.getLogger(AccountHandler.class);


  @Inject
  public AccountHandler(DataManagementService dataManagementService, AuthenticatorService authenticatorService) {
    this.dataManagementService = dataManagementService;
    this.authenticatorService = authenticatorService;
  }

  @Path("{id}")
  @GET
  @Produces("application/json")
  public void getAccountInfo(HttpRequest request, HttpResponder responder, @PathParam("id") int id) {

    requestReceived();

    try {
      Account account = dataManagementService.getAccount(id);
      if (account != null) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, account.toString());
      } else {
        requestFailed();
        LOG.error(String.format("Account not found. Processing endpoint: %s ", "GET /passport/v1/accounts"));
        responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getJsonError("Account not found"));
      }
    } catch (Exception e) {
      LOG.error(String.format("Error while processing end point %s. Error %s",
                              "GET /passport/v1/accounts", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", String.format("Exception while fetching account")));
    }
  }


  @Path("{id}/password")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public void changePassword(HttpRequest request, HttpResponder responder, @PathParam("id") int id) {

    try {
      requestReceived();
      String data = IOUtils.toString(new ChannelBufferInputStream(request.getContent()));

      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      String oldPassword = jsonObject.get("old_password") == null ? null : jsonObject.get("old_password").getAsString();
      String newPassword = jsonObject.get("new_password") == null ? null : jsonObject.get("new_password").getAsString();

      if ((oldPassword == null) || (oldPassword.isEmpty()) ||
        (newPassword == null) || (newPassword.isEmpty())) {
        requestFailed(); // Request failed
        LOG.error(String.format("Bad request no password supplied in endpoint %s",
                                "PUT /passport/v1/accounts/{id}/password"));
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             Utils.getJson("FAILED", "Must pass in old_password and new_password"));
      }

      dataManagementService.changePassword(id, oldPassword, newPassword);
      //Contract for the api is to return updated account to avoid a second call from the caller to get the
      // updated account
      Account account = dataManagementService.getAccount(id);
      if (account != null) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, account.toString());
      } else {
        requestFailed(); // Request failed
        LOG.error(String.format("Internal server error while processing endpoint: %s . %s",
                                "PUT /passport/v1/accounts/{id}/password", "Failed to get updated account"));
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                             Utils.getJson("FAILED", "Failed to get updated account"));
      }
    } catch (Exception e) {
      requestFailed(); // Request failed
      LOG.error(String.format("Exception while processing endpoint: %s  %s",
                              "PUT /passport/v1/accounts/{id}/password", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", String.format("Exception processing change password %s",
                                                                 e.getMessage())));
    }
  }

  @Path("{id}/downloaded")
  @PUT
  @Produces("application/json")
  public void confirmDownload(HttpRequest request, HttpResponder responder, @PathParam("id") int id) {
    requestReceived();

    try {
      dataManagementService.confirmDownload(id);
      //Contract for the api is to return updated account to avoid a second call from the caller to get the
      // updated account
      Account account = dataManagementService.getAccount(id);
      if (account != null) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, account.toString());
      } else {
        requestFailed(); // Request failed
        LOG.error(String.format("Internal server error while processing endpoint: %s. %s",
                                "PUT /passport/v1/accounts/{id}/downloaded", "Failed to fetch updated account"));
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                             Utils.getJson("FAILED", "Failed to get updated account"));
      }
    } catch (Exception e) {
      requestFailed(); // Request failed
      LOG.error(String.format("Internal server error while processing endpoint: %s. %s",
                              "PUT /passport/v1/account/{id}/downloaded", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", String.format("Download confirmation failed. %s", e.getMessage())));
    }
  }

  @Path("{id}/confirmPayment")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public void confirmPaymentInfoProvided(HttpRequest request, HttpResponder responder, @PathParam("id") int id) {
    requestReceived();

    try {
      String data = IOUtils.toString(new ChannelBufferInputStream(request.getContent()));

      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      String paymentAccountId = jsonObject.get("payments_account_id") == null
                                           ? null : jsonObject.get("payments_account_id").getAsString();

      if (paymentAccountId == null) {
        requestFailed();
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             Utils.getJson("FAILED", "Must pass payments_account_id in the input"));
        return;
      }

      dataManagementService.confirmPayment(id, paymentAccountId);
      //Contract for the api is to return updated account to avoid a second call from the caller to get the
      // updated account
      Account account = dataManagementService.getAccount(id);
      if (account != null) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, account.toString());
      } else {
        requestFailed(); // Request failed
        LOG.error(String.format("Internal server error while processing endpoint: %s. %s",
                                "PUT /passport/v1/accounts/{id}/downloaded", "Failed to fetch updated account"));
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                             Utils.getJson("FAILED", "Failed to get updated account"));
      }
    } catch (JsonParseException e) {
      requestFailed();
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           Utils.getJson("FAILED", "Failed to parse Json"));
    } catch (Exception e) {
      requestFailed(); // Request failed
      LOG.error(String.format("Internal server error while processing endpoint: %s. %s",
                              "PUT /passport/v1/accounts/{id}/downloaded", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", String.format("Download confirmation failed. %s", e.getMessage())));
    }
  }

  @Path("{id}")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public void updateAccount(HttpRequest request, HttpResponder responder, @PathParam("id") int id) {
    requestReceived();

   try {
     String data = IOUtils.toString(new ChannelBufferInputStream(request.getContent()));

     JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      Map<String, Object> updateParams = new HashMap<String, Object>();

      String firstName = jsonObject.get("first_name") == null ? null : jsonObject.get("first_name").getAsString();
      String lastName = jsonObject.get("last_name") == null ? null : jsonObject.get("last_name").getAsString();
      String company = jsonObject.get("company") == null ? null : jsonObject.get("company").getAsString();

      //TODO: Find a better way to update the map
      if (firstName != null) {
        updateParams.put("first_name", firstName);
      }

      if (lastName != null) {
        updateParams.put("last_name", lastName);
      }

      if (company != null) {
        updateParams.put("company", company);
      }

      dataManagementService.updateAccount(id, updateParams);
      //Contract for the api is to return updated account to avoid a second call from the caller to get the
      // updated account
      Account account = dataManagementService.getAccount(id);
      if (account != null) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, account.toString());
      } else {
        requestFailed(); // Request failed
        LOG.error(String.format("Internal server error while processing endpoint: %s .%s",
          "PUT /passport/v1/accounts/{id}", "Failed to get updated account"));
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                             Utils.getJson("FAILED", "Failed to get updated account"));
      }
    } catch (JsonParseException e) {
      requestFailed();
      LOG.error(String.format("Bad request while processing endpoint: %s %s",
                              "PUT /passport/v1/accounts/{id}", e.getMessage()));
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           Utils.getJson("FAILED", String.format("Json parse exception. %s", e.getMessage())));
    } catch (Exception e) {
      requestFailed(); // Request failed
      LOG.error(String.format("Internal server error endpoint: %s %s",
                              "PUT /passport/v1/accounts/{id}", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", String.format("Account Update Failed. %s", e.getMessage())));
      }
  }

  @POST
  @Produces("application/json")
  @Consumes("application/json")
  public void createAccount(HttpRequest request, HttpResponder responder) {
    requestReceived();

    String emailId = null;
    try {
      String data = IOUtils.toString(new ChannelBufferInputStream(request.getContent()));

      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      emailId = jsonObject.get("email_id") == null ? null : jsonObject.get("email_id").getAsString();

      if ((emailId == null)) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             Utils.getJson("FAILED", "Email id is missing"));
      } else {
        Account account = dataManagementService.registerAccount(new Account("", "", "", emailId));
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, account.toString());
      }
    } catch (AccountAlreadyExistsException e) {
      //If the account already exists - return the existing account so that the caller can take appropriate action
      Account account = dataManagementService.getAccount(emailId);
      requestFailed(); // Request failed
      LOG.error("Account creation failed endpoint: %s %s", "POST /passport/v1/accounts", "Account already exists");
      responder.sendString(HttpResponseStatus.CONFLICT, Utils.getJsonError("FAILED", account));
    } catch (JsonParseException e) {
      requestFailed();
      LOG.error(String.format("Bad request while processing endpoint: %s %s",
        "POST /passport/v1/accounts", e.getMessage()));
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           Utils.getJson("FAILED", String.format("Json parse exception. %s", e.getMessage())));
    } catch (Exception e) {
      requestFailed(); // Request failed
      LOG.error(String.format("Internal server error while processing endpoint: %s %s",
        "POST /passport/v1/accounts", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", String.format("Account Creation Failed. %s", e)));
    }
  }

  @Path("{id}/confirmed")
  @PUT
  @Produces("application/json")
  @Consumes("application/json")
  public void confirmAccount(HttpRequest request, HttpResponder responder, @PathParam("id") int id) {
    requestReceived();

    try {
      String data = IOUtils.toString(new ChannelBufferInputStream(request.getContent()));

      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      String accountPassword = jsonObject.get("password") == null ? null : jsonObject.get("password").getAsString();
      String firstName = jsonObject.get("first_name") == null ? null : jsonObject.get("first_name").getAsString();
      String lastName = jsonObject.get("last_name") == null ? null : jsonObject.get("last_name").getAsString();
      String company = jsonObject.get("company") == null ? null : jsonObject.get("company").getAsString();
      String emailId = jsonObject.get("email_id") == null ? null : jsonObject.get("email_id").getAsString();


      if ((accountPassword == null) || (accountPassword.isEmpty()) ||
          (emailId == null) || (emailId.isEmpty()) ||
          (firstName == null) || (firstName.isEmpty()) ||
          (lastName == null) || (lastName.isEmpty()) ||
          (company == null) || (company.isEmpty())) {
        requestFailed(); // Request failed
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             Utils.getJson("FAILED", "password, first_name, last_name, company should be passed in"));
      } else {
        Account account = new Account(firstName, lastName, company, emailId, id);
        dataManagementService.confirmRegistration(account, accountPassword);
        //Contract for the api is to return updated account to avoid a second call from the caller to get the
        // updated account
        Account accountFetched = dataManagementService.getAccount(id);
        if (accountFetched != null) {
          requestSuccess();
          responder.sendString(HttpResponseStatus.OK, accountFetched.toString());
        } else {
          requestFailed(); // Request failed
          LOG.error(String.format("Internal server error endpoint: %s %s ",
                                  "PUT /passport/v1/accounts/{id}/confirmed", "could not fetch updated account"));
          responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                               Utils.getJson("FAILED", "Failed to get updated account"));
        }
      }
    } catch (JsonParseException e) {
      requestFailed();
      LOG.error(String.format("Bad request while processing endpoint: %s %s",
        "PUT /passport/v1/accounts/{id}/confirmed", e.getMessage()));
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           Utils.getJson("FAILED", String.format("Json parse exception. %s", e.getMessage())));
    } catch (Exception e) {
      requestFailed(); // Request failed
      LOG.error(String.format("Internal server error while processing endpoint: %s %s",
        "PUT /passport/v1/accounts/{id}/confirmed", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", String.format("Account Confirmation Failed. %s", e)));
    }
  }


  @Path("{id}/clusters")
  @POST
  @Produces("application/json")
  @Consumes("application/json")
  public void createVPC(HttpRequest request, HttpResponder responder, @PathParam("id") int id) {
    requestReceived();

    try {
      String data = IOUtils.toString(new ChannelBufferInputStream(request.getContent()));

      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      String vpcName = jsonObject.get("vpc_name") == null ? null : jsonObject.get("vpc_name").getAsString();
      String vpcLabel = jsonObject.get("vpc_label") == null ? null : jsonObject.get("vpc_label").getAsString();
      String vpcType = jsonObject.get("vpc_type") == null ? "sandbox" : jsonObject.get("vpc_label").getAsString();

      if ((vpcName != null) && (!vpcName.isEmpty()) && (vpcLabel != null) && (!vpcLabel.isEmpty())) {
        VPC vpc = dataManagementService.addVPC(id, new VPC(vpcName, vpcLabel, vpcType));
        if (vpc != null) {
          requestSuccess();
          responder.sendString(HttpResponseStatus.OK , vpc.toString());
        } else {
          responder.sendString(HttpResponseStatus.BAD_REQUEST,
                               Utils.getJson("FAILED", String.format("VPC Creation failed. VPC name already exists")));
        }
      } else {
        requestFailed(); // Request failed
        LOG.error(String.format("Bad request while processing endpoint: %s %s",
          "POST /passport/v1/accounts/{id}/clusters", "Missing VPC name"));
        responder.sendString(HttpResponseStatus.BAD_REQUEST,
                             Utils.getJson("FAILED", "VPC creation failed. vpc_name is missing"));
      }
    } catch (JsonParseException e) {
      requestFailed();
      LOG.error(String.format("Bad request while processing endpoint: %s %s",
        "POST /passport/v1/accounts/{id}/clusters", e.getMessage()));
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           Utils.getJson("FAILED", String.format("Json parse exception. %s", e.getMessage())));
    } catch (Exception e) {
      requestFailed(); // Request failed
      LOG.error(String.format("Internal server error while processing endpoint: %s %s",
        "POST /passport/v1/accounts/{id}/clusters", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", String.format("VPC Creation Failed. %s", e)));
    }
  }

  @Path("{id}/clusters")
  @GET
  @Produces("application/json")
  public void getVPC(HttpRequest request, HttpResponder responder, @PathParam("id") int id) {
    requestReceived();

    try {
      List<VPC> vpcList = dataManagementService.getVPC(id);
      if (vpcList.isEmpty()) {
        responder.sendString(HttpResponseStatus.OK, "[]");
      } else {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        boolean first = true;
        for (VPC vpc : vpcList) {
          if (first) {
            first = false;
          } else {
            sb.append(",");
          }
          sb.append(vpc.toString());
        }
        sb.append("]");
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, sb.toString());
      }
    } catch (Exception e) {
      requestFailed(); // Request failed
      LOG.error(String.format("Internal server error while processing endpoint: %s %s",
        "GET /passport/v1/accounts/{id}/clusters", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJsonError(String.format("VPC get Failed. %s", e.getMessage())));
    }
  }

  @Path("{accountId}/clusters/{clusterId}")
  @GET
  @Produces("application/json")
  public void getSingleVPC(HttpRequest request, HttpResponder responder,
                           @PathParam("accountId") int accountId, @PathParam("clusterId") int vpcId) {
    requestReceived();

    try {
      VPC vpc = dataManagementService.getVPC(accountId, vpcId);
      if (vpc == null) {
        requestFailed(); // Request failed
        responder.sendString(HttpResponseStatus.NOT_FOUND,
                             Utils.getJsonError("VPC not found"));
      } else {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, vpc.toString());
      }
    } catch (Exception e) {
      requestFailed(); // Request failed
      LOG.error(String.format("Internal server error while processing endpoint: %s %s",
        "GET /passport/v1/accounts/{id}/clusters/{clusterId}", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJsonError(String.format("VPC get Failed. %s", e.getMessage())));
    }
  }

  @Path("authenticate")
  @POST
  @Produces("application/json")
  @Consumes("application/json")
  public void authenticate(HttpRequest request, HttpResponder responder) {

    //Logic -
    //  Either use emailId and password if present for auth
    //  if not present use ApiKey
    // If username and password is passed it can't be null
    // Dummy username and password is used if apiKey is passed to enable it to work with shiro

    requestReceived();
    try {
    String data = IOUtils.toString(new ChannelBufferInputStream(request.getContent()));
    String apiKey = request.getHeader("X-Continuuity-ApiKey");

    String emailId = UsernamePasswordApiKeyToken.DUMMY_USER;
    String password = UsernamePasswordApiKeyToken.DUMMY_PASSWORD;
    boolean useApiKey = true;

    if (data != null && !data.isEmpty()) {
      JsonParser parser = new JsonParser();
      JsonElement element = parser.parse(data);
      JsonObject jsonObject = element.getAsJsonObject();

      password = jsonObject.get("password") == null ? null
        : jsonObject.get("password").getAsString();
      emailId = jsonObject.get("email_id") == null ? null
        : jsonObject.get("email_id").getAsString();
      useApiKey = false;
    }

    if (emailId == null || emailId.isEmpty() || password == null || password.isEmpty()) {
      requestFailed();
      LOG.error(String.format("Bad request error while processing endpoint: %s %s",
        "POST /passport/v1/accounts/authenticate", "Empty email or password fields"));
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           Utils.getAuthenticatedJson("Bad Request.", "Username and password can't be null"));
      return;
    }

    UsernamePasswordApiKeyToken token = null;
    if (useApiKey) {
      token = new UsernamePasswordApiKeyToken(UsernamePasswordApiKeyToken.DUMMY_USER,
        UsernamePasswordApiKeyToken.DUMMY_PASSWORD,
        apiKey, true);
    } else {
      String hashed = PasswordUtils.generateHashedPassword(password);
      token = new UsernamePasswordApiKeyToken(emailId,
        hashed, apiKey, false);
    }



      AuthenticationStatus status = authenticatorService.authenticate(token);
      if (status.getType().equals(AuthenticationStatus.Type.AUTHENTICATED)) {
        //TODO: Better naming for authenticatedJson?
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, status.getMessage());
      } else {
        requestFailed(); //Failed request
        LOG.error(String.format("Unauthorized while processing endpoint: %s %s",
          "POST /passport/v1/accounts/authenticate", "User doesn't exist or password doesn't match"));
        responder.sendString(HttpResponseStatus.UNAUTHORIZED,
                             Utils.getAuthenticatedJson("Authentication Failed.",
                                                        "Either user doesn't exist or password doesn't match"));
      }
    } catch (Exception e) {
      requestFailed(); //Failed request
      LOG.error(String.format("Unauthorized while processing endpoint: %s %s",
        "POST /passport/v1/accounts/authenticate", e.getMessage()));
      responder.sendString(HttpResponseStatus.UNAUTHORIZED,
                           Utils.getAuthenticatedJson("Authentication Failed.", e.getMessage()));
    }
  }

  @Path("{id}/regenerateApiKey")
  @POST
  @Produces("application/json")
  public void regenerateApiKey(HttpRequest request, HttpResponder responder, @PathParam("id") int accountId) {
    try {
      dataManagementService.regenerateApiKey(accountId);
      //Contract for the api is to return updated account to avoid a second call from the caller to get the
      // updated account
      Account accountFetched = dataManagementService.getAccount(accountId);
      if (accountFetched != null) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, accountFetched.toString());
      } else {
        requestFailed(); // Request failed
        responder.sendString(HttpResponseStatus.NOT_FOUND,
                             Utils.getJson("FAILED", "Failed to get regenerate key. Account not found"));
      }
    } catch (Exception e) {
      requestFailed(); // Request failed
      LOG.error(String.format("Internal server error while processing endpoint: %s %s",
        "GET /passport/v1/accounts/{id}/regenerateApiKey", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJson("FAILED", "Failed to get regenerate key"));
    }
  }

  @Path("{id}")
  @DELETE
  @Produces("application/json")
  public void deleteAccount(HttpRequest request, HttpResponder responder, @PathParam("id") int id) {
    requestReceived();

    try {
      dataManagementService.deleteAccount(id);
      requestSuccess();
      responder.sendString(HttpResponseStatus.OK, Utils.getJsonOK());
    } catch (AccountNotFoundException e) {
      requestFailed(); //Failed request
      LOG.error(String.format("Account not found endpoint: %s %s",
        "DELETE /passport/v1/accounts/{id}", e.getMessage()));
      responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getJsonError("Account not found"));
    } catch (RuntimeException e) {
      requestFailed(); //Failed request
      LOG.error(String.format("Internal server error while processing endpoint: %s %s",
        "DELETE /passport/v1/accounts/{id}", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJsonError("Account delete Failed", e.getMessage()));
    }
  }

  @Path("{accountId}/clusters/{clusterId}")
  @DELETE
  @Produces("application/json")
  public void deleteVPC(HttpRequest request, HttpResponder responder,
                        @PathParam("accountId") int accountId, @PathParam("clusterId") int vpcId) {
    requestReceived();

    try {
      dataManagementService.deleteVPC(accountId, vpcId);
      requestSuccess();
      responder.sendString(HttpResponseStatus.OK, Utils.getJsonOK());
    } catch (VPCNotFoundException e) {
      requestFailed(); //Failed request
      LOG.error(String.format("VPC not found endpoint: %s %s",
        "DELETE /passport/v1/accounts/{id}/clusters/{clusterId}", e.getMessage()));
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           Utils.getJsonError("VPC not found"));
    } catch (RuntimeException e) {
      requestFailed(); //Failed request
      LOG.error(String.format("Internal server error endpoint: %s %s",
        "DELETE /passport/v1/accounts/{id}/clusters/{clusterId}", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJsonError("VPC delete Failed", e.getMessage()));
    }
  }

  @Path("{accountId}/organizations/{orgId}")
  @PUT
  public void updateOrganization(HttpRequest request, HttpResponder responder,
                                 @PathParam("orgId") String orgId, @PathParam("accountId") int accountId) {
    requestReceived();
    try {
      dataManagementService.updateAccountOrganization(accountId, orgId);
      // Contract for the api is to return updated account to avoid a second call from the caller to get the
      // updated account
      Account accountFetched = dataManagementService.getAccount(accountId);
      if (accountFetched != null) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, accountFetched.toString());
      } else {
        requestFailed(); // Request failed
        LOG.error("Internal server error endpoint: {} {} ", "PUT /passport/v1/accounts/{id}/organizations/{orgId}",
                  "could not fetch updated account");
        responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                             Utils.getJson("FAILED", "Failed to get updated account"));
      }
    } catch (AccountNotFoundException e) {
      requestFailed(); //Failed request
      responder.sendString(HttpResponseStatus.NOT_FOUND,
                           Utils.getJsonError("Account to be updated not found"));
    } catch (OrganizationNotFoundException e) {
      requestFailed(); //Failed request
      responder.sendString(HttpResponseStatus.CONFLICT,
                           Utils.getJsonError("Organization not found in the system"));
    } catch (Throwable e) {
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJsonError("Error while updating the org.", e.getMessage()));
    }
  }

  @Override
  public void init(HandlerContext context) {
  }

  @Override
  public void destroy(HandlerContext context) {
  }
}
