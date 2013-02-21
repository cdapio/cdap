/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.services;

import com.continuuity.app.authorization.AuthorizationFactory;
import com.continuuity.app.deploy.ManagerFactory;
import com.continuuity.app.services.AppFabricServerFactory;
import com.continuuity.app.services.AppFabricService;
import com.continuuity.app.store.StoreFactory;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.filesystem.LocationFactory;
import com.google.inject.Inject;

/**
 * An concrete implementation of simple {@link com.continuuity.app.services.AppFabricServerFactory}.
 * <p>
 *   This implementation creates the default version of the server used
 *   for deployment of archives and management of those.
 * </p>
 */
public class InMemoryAppFabricServerFactory implements AppFabricServerFactory {
  private final OperationExecutor opex;
  private final LocationFactory lFactory;
  private final ManagerFactory mFactory;
  private final AuthorizationFactory aFactory;
  private final StoreFactory sFactory;

  @Inject
  public InMemoryAppFabricServerFactory(OperationExecutor opex, LocationFactory lFactory, ManagerFactory mFactory,
                                        AuthorizationFactory aFactory, StoreFactory sFactory) {
    this.opex = opex;
    this.lFactory = lFactory;
    this.mFactory = mFactory;
    this.aFactory = aFactory;
    this.sFactory = sFactory;
  }

  @Override
  public AppFabricService.Iface create(CConfiguration configuration) {
    return new AppFabricServer(configuration, opex, lFactory, mFactory, aFactory, sFactory);
  }
}
