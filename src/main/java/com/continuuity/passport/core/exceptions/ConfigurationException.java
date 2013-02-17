package com.continuuity.passport.core.exceptions;

/**
 *
 */
public class ConfigurationException extends  Exception {
  public ConfigurationException(String message) {
    super(message);
  }

  public ConfigurationException(String message, Exception cause) {
    super(message, cause);
  }
}
