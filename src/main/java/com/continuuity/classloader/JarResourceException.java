/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.classloader;

/**
 * Raised when there is issue with jar decompressing and locating resource.
 * Reason is inlcuded in the exception.
 */
public class JarResourceException extends Exception {
  public JarResourceException(String reason) {
    super(reason);
  }
}
