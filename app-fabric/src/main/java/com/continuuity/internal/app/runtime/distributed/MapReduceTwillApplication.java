/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.mapreduce.MapReduceSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import org.apache.twill.api.EventHandler;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.filesystem.Location;

import java.io.File;

/**
 * {@link TwillApplication} to run {@link MapReduceTwillRunnable}
 */
public final class MapReduceTwillApplication implements TwillApplication {

  private final MapReduceSpecification spec;
  private final Program program;
  private final File hConfig;
  private final File cConfig;
  private final EventHandler eventHandler;

  public MapReduceTwillApplication(Program program, MapReduceSpecification spec,
                                   File hConfig, File cConfig, EventHandler eventHandler) {
    this.spec = spec;
    this.program = program;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
    this.eventHandler = eventHandler;
  }

  @Override
  public TwillSpecification configure() {
    // These resources are for the container that runs the mapred client that will launch the actual mapred job.
    // It does not need much memory.  Memory for mappers and reduces are specified in the MapReduceSpecification,
    // which is configurable by the author of the job.
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(1)
      .build();

    Location programLocation = program.getJarLocation();

    return TwillSpecification.Builder.with()
      .setName(String.format("%s.%s.%s.%s",
                             Type.MAPREDUCE.name().toLowerCase(),
                             program.getAccountId(), program.getApplicationId(), spec.getName()))
      .withRunnable()
        .add(spec.getName(),
             new MapReduceTwillRunnable(spec.getName(), "hConf.xml", "cConf.xml"),
             resourceSpec)
        .withLocalFiles()
          .add(programLocation.getName(), programLocation.toURI())
          .add("hConf.xml", hConfig.toURI())
          .add("cConf.xml", cConfig.toURI()).apply()
      .anyOrder().withEventHandler(eventHandler).build();
  }
}
