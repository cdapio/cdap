package com.continuuity.data2.dataset.lib.table;

/**
 * Allows checking if TTL is supported.
 */
// todo: remove this interface it is redundant. Refactor
public interface TimeToLiveSupported {
  static final String PROPERTY_TTL = "ttl";
  // todo: find a better way to expose properties of data sets

  /**
   * Returns true if TTL is supported, false otherwise.
   *
   * This method is needed for delegation.
   */
  boolean isTTLSupported();
}
