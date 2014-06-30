package com.continuuity.data2.dataset.lib.table;

/**
 * Represents a write to the datastore.
 * @param <T> The type of value for the update.
 */
public interface Update<T> {
  T getValue();

  byte[] getBytes();
}
