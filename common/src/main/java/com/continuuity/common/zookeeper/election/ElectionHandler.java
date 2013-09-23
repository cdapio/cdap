package com.continuuity.common.zookeeper.election;

/**
 * Handles events of election/un-election of leader.
 */
public interface ElectionHandler {

  /**
   * This method will get invoked when a participant becomes a leader in a
   * leader election process. It is guaranteed that this method won't get called
   * consecutively (i.e. called twice or more in a row).
   */
  void leader();

  /**
   * This method will get invoked when a participant is a follower in a
   * leader election process. This method might get called multiple times without
   * the {@link #leader()} method being called.
   */
  void follower();
}
