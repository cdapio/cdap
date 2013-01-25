package com.continuuity.passport.core;

import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Set;


/**
 * Identifies User entity in the system
 */

public class User {
  public enum RoleType {OWNER, ADMIN, USER}

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

  private String saltedHashedPassword;

  private Set<Authority> authorities = new HashSet<Authority>();

  public String getPassword() {
    return saltedHashedPassword;
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

  /**
   * get all the authorities that the user is a part of
   * authorities define the account id and role the user has on the account
   *
   * @return ImmutableSet of authority
   */
  public Set<Authority> getAuthorities() {
    return ImmutableSet.copyOf(this.authorities);
  }

  private void addAuthority(Authority authority) {
    this.authorities.add(authority);
  }


  //TODO: Add Builders to create build the object
  public static class Builder {
  }

}
