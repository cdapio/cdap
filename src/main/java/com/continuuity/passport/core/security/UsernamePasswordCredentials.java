package com.continuuity.passport.core.security;

import com.continuuity.passport.core.security.Credentials;

/**
 *
 */
public class UsernamePasswordCredentials extends Credentials {

  private String user_name;

  private String password;

  public UsernamePasswordCredentials(String userName, String password) {
    this.user_name = userName;
    this.password = password;
  }

  public String getUserName() {
    return user_name;
  }

  public String getPassword() {
    return password;
  }
}
