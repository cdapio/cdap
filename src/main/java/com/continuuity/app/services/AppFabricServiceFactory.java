/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.services;

import com.continuuity.common.conf.CConfiguration;

/**
 * An interface defines away to create a Server factory.
 */
public interface AppFabricServiceFactory {
  /**
   * Creates an instance of {@link AppFabricService.Iface}.
   *
   * @param configuration to be used for configuring
   * @return An instance of {@link AppFabricService.Iface}
   */
  public AppFabricService.Iface create(CConfiguration configuration);
}
