/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.internal.app.runtime.webapp.WebappProgramRunner;
import com.google.common.base.Throwables;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.Location;

import java.io.File;


/**
 * Twill application wrapper for webapp.
 */
public final class WebappTwillApplication implements TwillApplication {

  private final Program program;
  private final File hConfig;
  private final File cConfig;
  private final EventHandler eventHandler;

  public WebappTwillApplication(Program program, File hConfig, File cConfig, EventHandler eventHandler) {
    this.program = program;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
    this.eventHandler = eventHandler;
  }

  @Override
  public TwillSpecification configure() {
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(1)
      .build();

    Location programLocation = program.getJarLocation();

    try {
      String serviceName = WebappProgramRunner.getServiceName(Type.WEBAPP, program);

      return TwillSpecification.Builder.with()
        .setName(serviceName)
        .withRunnable()
          .add(serviceName, new WebappTwillRunnable(serviceName, "hConf.xml", "cConf.xml"),
               resourceSpec)
          .withLocalFiles()
            .add(programLocation.getName(), programLocation.toURI())
            .add("hConf.xml", hConfig.toURI())
            .add("cConf.xml", cConfig.toURI()).apply()
        .anyOrder().withEventHandler(eventHandler).build();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
