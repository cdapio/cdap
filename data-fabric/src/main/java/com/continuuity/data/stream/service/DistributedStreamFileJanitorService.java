/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream.service;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.zookeeper.election.ElectionHandler;
import com.continuuity.common.zookeeper.election.LeaderElection;
import com.continuuity.data.stream.StreamFileJanitor;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.twill.zookeeper.ZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs leader election and the leader will do stream file cleanup using {@link LocalStreamFileJanitorService}.
 */
@Singleton
public final class DistributedStreamFileJanitorService extends AbstractIdleService implements StreamFileJanitorService {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedStreamFileJanitorService.class);
  private final LeaderElection leaderElection;

  @Inject
  public DistributedStreamFileJanitorService(ZKClient zkClient,
                                             final StreamFileJanitor janitor,
                                             final CConfiguration cConf) {

    String electionPrefix = "/" + Constants.Service.STREAMS + "/leader";
    leaderElection = new LeaderElection(zkClient, electionPrefix, new ElectionHandler() {

      Service janitorService;

      @Override
      public void leader() {
        LOG.info("Leader of stream file janitor service");
        janitorService = new LocalStreamFileJanitorService(janitor, cConf);
        janitorService.start();
      }

      @Override
      public void follower() {
        LOG.info("Follower of stream file janitor service");
        if (janitorService != null) {
          janitorService.stop();
          janitorService = null;
        }
      }
    });
  }

  @Override
  protected void startUp() throws Exception {
    leaderElection.startAndWait();
  }

  @Override
  protected void shutDown() throws Exception {
    leaderElection.stopAndWait();
  }
}
