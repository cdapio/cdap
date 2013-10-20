/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.internal.app.runtime.webapp.WebappProgramRunner;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Throwables;

import java.io.File;

/**
 * Weave application wrapper for webapp.
 */
public final class WebappWeaveApplication implements WeaveApplication {

  private final Program program;
  private final File hConfig;
  private final File cConfig;

  public WebappWeaveApplication(Program program, File hConfig, File cConfig) {
    this.program = program;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
  }

  @Override
  public WeaveSpecification configure() {
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(1)
      .build();

    Location programLocation = program.getJarLocation();

    try {
      String serviceName = WebappProgramRunner.getServiceName(Type.WEBAPP, program);

      return WeaveSpecification.Builder.with()
        .setName(serviceName)
        .withRunnable()
          .add(serviceName, new WebappWeaveRunnable(serviceName, "hConf.xml", "cConf.xml"),
               resourceSpec)
          .withLocalFiles()
            .add(programLocation.getName(), programLocation.toURI())
            .add("hConf.xml", hConfig.toURI())
            .add("cConf.xml", cConfig.toURI()).apply()
        .anyOrder().build();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
