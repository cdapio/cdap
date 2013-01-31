package com.continuuity.data;

/**
 * A batch collection registry is used to manage the clients that need
 * to be notified when the batch collector changes. Clients use this
 * interface to subscribe to the notification.
 */
public interface BatchCollectionRegistry {

  /**
   * Adds a batch collection client to the registry
   * @param client {@link BatchCollectionClient}
   */
  public void register(BatchCollectionClient client);
}
