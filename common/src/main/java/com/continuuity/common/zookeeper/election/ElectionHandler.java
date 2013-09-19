package com.continuuity.common.zookeeper.election;

/**
 * Handles events of election/un-election of leader.
 */
public interface ElectionHandler {

  void elected(String id);

  void unelected(String id);
}
