/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.services;

import com.continuuity.common.conf.CConfiguration;

/**
 * An interface defines away to create a Server factory.
 */
public interface DeploymentServerFactory {
  /**
   * Creates an instance of {@link DeploymentService.Iface}.
   *
   * @param configuration to be used for configuring
   * @return An instance of {@link DeploymentService.Iface}
   */
  public DeploymentService.Iface create(CConfiguration configuration);
}
