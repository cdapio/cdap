/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.deploy;

import com.continuuity.app.deploy.Manager;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.pipeline.PipelineFactory;
import com.google.inject.Inject;

/**
 *
 */
public final class SyncManagerFactory implements ManagerFactory {
  private final PipelineFactory<?> pFactory;
  private final LocationFactory lFactory;
  private final StoreFactory sFactory;

  @Inject
  public SyncManagerFactory(PipelineFactory<?> pFactory, LocationFactory lFactory, StoreFactory sFactory) {
    this.pFactory = pFactory;
    this.lFactory = lFactory;
    this.sFactory = sFactory;
  }

  @Override
  public Manager<?, ?> create(CConfiguration configuration) {
    return new LocalManager(configuration, pFactory, lFactory, sFactory);
  }
}
