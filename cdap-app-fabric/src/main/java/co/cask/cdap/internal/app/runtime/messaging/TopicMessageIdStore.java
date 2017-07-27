package co.cask.cdap.internal.app.runtime.messaging;

import javax.annotation.Nullable;

/**
 * Defines the interface for retrieving and persisting topic - messageId key-value pairs from storage.
 */
public interface TopicMessageIdStore {
  /**
   * Gets the messageId that was previously set for a given topic.
   *
   * @return the messageId, or null if no messageId was previously associated with the given topic
   */
  @Nullable
  String retrieveSubscriberState(String topic);

  /**
   * Sets a messageId to be associated with a given topic.
   */
  void persistSubscriberState(String topic, String messageId);
}
