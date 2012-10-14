package com.continuuity.metrics2.temporaldb;

/**
 * Interfaces for a Key value store.
 */
public interface KVStore {
  /**
   * Stores a value for a given key.
   *
   * @param key under which the <code>value</code> is stored.
   * @param value that will be stored under <code>key</code>
   * @throws Exception
   */
  public void put(byte[] key, byte[] value) throws Exception;

  /**
   * Gets the value based on the <code>key</code> provided.
   * @param key
   * @return byte array of value associated with key.
   * @throws Exception
   */
  public byte[] get(byte[] key) throws Exception;
}
