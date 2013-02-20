package com.continuuity.passport.http.handlers;

/**
 *
 */
public class Utils {

  //TODO: Clean up lot of these helper functions
  public static String getJsonError(String error) {
    return String.format("{\"error\" :, \"%s\" }", error);
  }

  public static String getJsonError(String error, Exception e) {
    return String.format("{\"error\":, \"%s . %s \" }", error, e.getMessage());
  }

  public static String getJsonError(String error, String reason) {
    return String.format("{\"error\":, \"%s , \"reason\" : \"%s \" }", error, reason);
  }

  public static String getJson(String status, String message) {
    return String.format("{\"status\":\"%s\",\"message\":\"%s\" }", status, message);
  }

  public static String getJson(String status, String message, Exception e) {
    String errorMessage = String.format("%s. %s", message, e.getMessage());
    return String.format("{\"status\":\"%s\",\"message\":\"%s\" }", status, errorMessage);
  }

  public static String getAuthenticatedJson(String result) {
    return String.format("{\"error\": null, \"result\": \"%s\" }", result);
  }

  public static String getAuthenticatedJson(String error, String result) {
    return String.format("{\"error\": \"%s\", \"result\": \" %s \" }", error, result);
  }

  public static String getNonceJson(int result) {
    return String.format("{\"error\": null, \"result\": %d }", result);
  }

  public static String getNonceJson(String error, int result) {
    return String.format("{\"error\": \"%s\", \"result\": %d }", error, result);
  }

  public static String getJsonOK() {
    return String.format("{\"error\": null }");
  }


}
