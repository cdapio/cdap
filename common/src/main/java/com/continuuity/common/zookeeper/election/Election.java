package com.continuuity.common.zookeeper.election;

import com.google.common.base.Objects;

/**
 * Represents leader election.
 */
public class Election {
  private final String id;
  private final ElectionHandler electionHandler;

  public Election(String id, ElectionHandler electionHandler) {
    this.id = id;
    this.electionHandler = electionHandler;
  }

  public Election(Election election) {
    this.id = election.getId();
    this.electionHandler = election.getElectionHandler();
  }

  public String getId() {
    return id;
  }

  public ElectionHandler getElectionHandler() {
    return electionHandler;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Election election = (Election) o;

    return id.equals(election.id);

  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("id", id)
      .add("electionHandler", electionHandler)
      .toString();
  }
}
