package com.continuuity.passport.core;

import java.util.HashSet;
import java.util.Set;


/**
 * Identifies User entity in the system
 *
 */

public class User  {
  public enum RoleType{OWNER,ADMIN,USER}

  public class UserRole {

    private RoleType roleType;
    private String userId;

  }

  public class Authority {
    private RoleType role;
    private String accountId;
  }


  private String firstName;
  private String lastName;
  private String emailId;
  private String password;

  private Set<Authority> authorities = new HashSet<Authority>();

  public String getPassword() {
    return password;
  }
  public String getFirstName() {
    return firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public String getEmailId() {
    return emailId;
  }

  public Set<Authority> getAuthorities() {
    return this.authorities;
  }

  private void addAuthority(Authority authority){
    this.authorities.add(authority);
  }


  //TODO: Add Builders to create build the object
  public static class Builder {
  }

}
