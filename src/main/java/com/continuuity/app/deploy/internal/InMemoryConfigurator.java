/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy.internal;

import com.continuuity.app.deploy.Configurator;

/**
 *
 */
public class InMemoryConfigurator implements Configurator  {
  private final String jarFilename;

  public InMemoryConfigurator(String jarFilename) {
    this.jarFilename = jarFilename;
  }

  @Override
  public Response call() throws Exception {
    // Load the JAR.
    // Invoke configure();
    // Return JSON.
    return null;
  }

  @Override
  public void destory() {
    return;
  }

}
