/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.pipeline.PipelineFactory;
import com.continuuity.weave.filesystem.LocationFactory;
import com.google.inject.Inject;

/**
 *
 */
public final class SyncManagerFactory implements ManagerFactory {
  private final CConfiguration configuration;
  private final PipelineFactory<?> pFactory;
  private final LocationFactory lFactory;
  private final StoreFactory sFactory;

  @Inject
  public SyncManagerFactory(CConfiguration configuration, PipelineFactory<?> pFactory,
                            LocationFactory lFactory, StoreFactory sFactory) {
    this.configuration = configuration;
    this.pFactory = pFactory;
    this.lFactory = lFactory;
    this.sFactory = sFactory;
  }

  @Override
  public Manager<?, ?> create() {
    return new LocalManager(configuration, pFactory, lFactory, sFactory);
  }
}
