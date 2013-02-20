package com.continuuity.data.operation.ttqueue;

import java.util.Map;

/**
 * Interface for queue entries.
 */
public interface QueueEntry {


  /**
   * Method to add a partitioning key and a hash value to the queue entry.
   * @param key the partitioning key
   * @param hash the hash value
   */
  public void addPartitioningKey(String key, int hash);

  /**
   * Method to get the hash value for a provided key
   * @param key the partitioning key
   * @return the hash value of the key or null if key was not found
   */
  public Integer getHash(String key);

  /**
   * Method to get the data from the queue entry
   * @return the data of the queue entry
   */
  public byte[] getData();

  /**
   * Method to set the data field of the queue entry
   * @param data the data byte array
   */
  public void setData(byte[] data);

  /**
   * Method to get all partitioning keys and hash values
   * @return a map of all keys and values
   */
  public Map<String, Integer> getPartitioningMap();
}