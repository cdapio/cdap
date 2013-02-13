package com.continuuity.passport.http;

import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.impl.DataManagementServiceImpl;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.shiro.util.StringUtils;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 *
 */
public class AccountHandler extends HttpServlet {

  private DataManagementService dataManagementService = new DataManagementServiceImpl();

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {


    String id = request.getParameter("id");
    Account account  = null ;

    if(id != null && !id.isEmpty()) {
      account = this.dataManagementService.getAccount(id);
    }

    if (account != null ){
      response.setContentType("application/json");
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().println(account.toString());

    }
    else {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
    }

  }
  @Override
  public void doPut(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
    ServletInputStream inputStream = request.getInputStream();
    int length  =  inputStream.available();

    byte [] buffer = new byte[length] ;
    inputStream.read(buffer);

    try{
      createAccount(new String(buffer));
      response.setContentType("application/json;charset=utf-8");
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().println("{\"message\": \"Account Created\"}");

    }
    catch (Exception e) {
      response.setContentType("application/json");
      response.setStatus(HttpServletResponse.SC_EXPECTATION_FAILED);
      response.getWriter().println("{\"message\": \"Account creation failed\", \"reason\": \""+e.getMessage()+"\"}");

    }

  }

  private boolean createAccount(String data) throws RuntimeException {
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

    if ( (accountEmail == StringUtils.EMPTY_STRING) || (accountName == StringUtils.EMPTY_STRING) ){
      throw new RuntimeException("Input json does not contain email_id or name fields");
    }
    else {
      dataManagementService.registerAccount(new Account(accountName,accountEmail));
    }

    return true;

  }


}
