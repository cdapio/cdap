package com.continuuity.api.data;

/**
 * this interface defined a single method used to provide a client
 * with a new batch collector. Every time this method is called, the
 * client must give up all references to its current batch collector
 * and start using the new, provided collector.
 */
public interface BatchCollectionClient {

  /**
   * Set a new batch collector
   * @param collector the new batch collector
   */
  public void setCollector(BatchCollector collector);

  /**
   * Get the current batch collector
   * @return the current batch collector
   */
  public BatchCollector getCollector();

}
