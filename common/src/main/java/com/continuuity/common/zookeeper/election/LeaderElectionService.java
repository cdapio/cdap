package com.continuuity.common.zookeeper.election;

/**
 * Service that does leader election.
 */
public interface LeaderElectionService {

  void registerElection(Election election);
}
