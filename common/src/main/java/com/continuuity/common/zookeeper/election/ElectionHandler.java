package com.continuuity.common.zookeeper.election;

/**
 * Handles events of election/un-election of leader.
 */
public interface ElectionHandler {

  void leader();

  void follower();
}
