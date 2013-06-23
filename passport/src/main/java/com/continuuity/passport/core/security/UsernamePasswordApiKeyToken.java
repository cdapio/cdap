/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.core.security;

import org.apache.shiro.authc.UsernamePasswordToken;

/**
 * Defines UsernamePassword credentials extends shiro UsernamePasswordToken to use shiro for authentication.
 */
public class UsernamePasswordApiKeyToken extends UsernamePasswordToken implements Credentials {
  public static final String DUMMY_USER = "continuuity";
  public static final String DUMMY_PASSWORD = "continuuity";

  private final String apiKey;
  private final boolean useApiKey;

  public UsernamePasswordApiKeyToken(String username, String password, String apiKey, boolean useApiKey) {
    super(username, password);
    this.apiKey = apiKey;
    this.useApiKey  = useApiKey;
  }

  public boolean isUseApiKey() {
    return useApiKey;
  }

  public String getApiKey() {
    return apiKey;
  }
}
