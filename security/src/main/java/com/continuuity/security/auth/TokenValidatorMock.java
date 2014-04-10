package com.continuuity.security.auth;

/**
 * Mock validator that always return valid state. This is mainly written for unit tests to go through for Netty Router.
 */
public class TokenValidatorMock implements Validator{
  @Override
  public State validate(String token){
    return State.TOKEN_VALID;
  }

}
