/*
 * Copyright (c) 2012-2013 Continuuity Inc. All rights reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.app.services.DeploymentService;
import com.continuuity.app.services.RuntimeService;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.continuuity.common.zookeeper.ZookeeperClientProvider;
import com.continuuity.filesystem.LocationFactory;
import com.continuuity.internal.filesystem.LocalLocationFactory;
import com.google.inject.AbstractModule;
import com.netflix.curator.framework.CuratorFramework;

public class ServicesModule4Test extends AbstractModule {
  private final CConfiguration conf;

  public ServicesModule4Test(final CConfiguration conf) {
    this.conf = conf;
  }

  @Override
  protected void configure() {
    bind(RuntimeService.Iface.class).to(RuntimeServiceImpl.class);
    //bind(DeploymentService.Iface.class).to(DeploymentServiceImpl.class);
    bind(LocationFactory.class).to(LocalLocationFactory.class);

    String zkEnsemble = conf.get(Constants.CFG_ZOOKEEPER_ENSEMBLE, Constants.DEFAULT_ZOOKEEPER_ENSEMBLE);
    CuratorFramework curatorClient = ZookeeperClientProvider.getClient(zkEnsemble, 1800, 1000);
    bind(CuratorFramework.class).toInstance(curatorClient);
  }
}
