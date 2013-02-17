package com.continuuity.passport.core.meta;

/**
 *
 */
public class UsernamePasswordApiKeyCredentials extends UsernamePasswordCredentials {

  private final String apiKey;

  public UsernamePasswordApiKeyCredentials(String userName, String password, String apiKey) {
    super(userName, password);
    this.apiKey = apiKey;
  }

  public String getApiKey() {
    return apiKey;
  }
}
