/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.app.services.AppFabricServiceFactory;
import com.continuuity.app.services.ServerFactory;
import com.continuuity.common.conf.CConfiguration;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;

/**
 *
 */
public class AppFabricServerFactory implements ServerFactory {
  private final AppFabricServiceFactory factory;

  @Inject
  public AppFabricServerFactory(AppFabricServiceFactory factory) {
    this.factory = factory;
  }

  @Override
  public Service create(CConfiguration configuration) {
    return new AppFabricServer(factory, configuration);
  }
}
