package com.continuuity.passport.http;

/**
 *
 */
public class Utils {


  public static String getJson(String status, String message){

    return String.format("{\"status\":\"%s\",\"message\":\"%s\" }",status, message);
  }

  public static String getJson(String status, String message, long id){

    return String.format("{\"status\":\"%s\",\"message\":\"%s\", \"id\": %d }",status, message,id);
  }

  public static String getJson(String status, String message, Exception e){

    String errorMessage =  String.format("%s. %s",message,e.getMessage()) ;
    return String.format("{\"status\":\"%s\",\"message\":\"%s\" }",status, errorMessage);
  }

  public static String getBasicAuthParam(String authorization) {

    String [] params = authorization.split("Basic ");

    if (params.length < 2 ){
      return null;
    }
    else {
      return params[1];
    }
  }

}
