/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.dataset2.manager.DatasetManager;
import com.continuuity.data2.transaction.queue.QueueAdmin;
import com.continuuity.pipeline.PipelineFactory;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocationFactory;

/**
 *
 */
public final class SyncManagerFactory implements ManagerFactory {
  private final CConfiguration configuration;
  private final PipelineFactory pFactory;
  private final LocationFactory lFactory;
  private final StoreFactory sFactory;
  private final QueueAdmin queueAdmin;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final DatasetManager datasetManager;

  @Inject
  public SyncManagerFactory(CConfiguration configuration, PipelineFactory pFactory,
                            LocationFactory lFactory, StoreFactory sFactory, QueueAdmin queueAdmin,
                            DiscoveryServiceClient discoveryServiceClient, DatasetManager datasetManager) {
    this.configuration = configuration;
    this.pFactory = pFactory;
    this.lFactory = lFactory;
    this.sFactory = sFactory;
    this.queueAdmin = queueAdmin;
    this.discoveryServiceClient = discoveryServiceClient;
    this.datasetManager = datasetManager;
  }

  @Override
  public <I, O> Manager<I, O> create(ProgramTerminator programTerminator) {
    return new LocalManager<I, O>(configuration, pFactory, lFactory, sFactory, programTerminator, queueAdmin,
                            discoveryServiceClient, datasetManager);
  }
}
