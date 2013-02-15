package com.continuuity.passport.core.meta;

/**
 *
 */
public class UsernamePasswordCredentials extends Credentials {

  private String userName;

  private String password;

  public UsernamePasswordCredentials(String userName, String password) {
    this.userName = userName;
    this.password = password;
  }

  public String getUserName() {
    return userName;
  }

  public String getPassword() {
    return password;
  }
}
