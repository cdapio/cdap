package com.continuuity.common.zookeeper.election;

import com.continuuity.weave.common.Cancellable;

/**
 * Service that does leader election.
 */
public interface LeaderElectionService {

  Cancellable addElection(Election election);
}
