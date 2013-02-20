package com.continuuity.passport.core.security;

import org.apache.shiro.authc.UsernamePasswordToken;

/**
 * This is extended from Shiro
 */
public class UsernamePasswordApiKeyToken extends UsernamePasswordToken implements Credentials {

  private final String apiKey;


  public UsernamePasswordApiKeyToken(String username, String password, String apiKey) {
    super(username, password);
    this.apiKey = apiKey;
  }

  public String getApiKey() {
    return apiKey;
  }
}
