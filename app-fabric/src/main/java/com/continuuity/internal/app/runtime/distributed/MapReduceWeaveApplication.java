/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.batch.MapReduceSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.filesystem.Location;

import java.io.File;

/**
 * {@link WeaveApplication} to run {@link MapReduceWeaveRunnable}
 */
public final class MapReduceWeaveApplication implements WeaveApplication {

  private final MapReduceSpecification spec;
  private final Program program;
  private final File hConfig;
  private final File cConfig;

  public MapReduceWeaveApplication(Program program, MapReduceSpecification spec, File hConfig, File cConfig) {
    this.spec = spec;
    this.program = program;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
  }

  @Override
  public WeaveSpecification configure() {
    // These resources are for the container that runs the mapred client that will launch the actual mapred job.
    // It does not need much memory.  Memory for mappers and reduces are specified in the MapReduceSpecification,
    // which is configurable by the author of the job.
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(1)
      .build();

    Location programLocation = program.getJarLocation();

    return WeaveSpecification.Builder.with()
      .setName(String.format("%s.%s.%s.%s",
                             Type.MAPREDUCE.name(), program.getAccountId(), program.getApplicationId(), spec.getName()))
      .withRunnable()
        .add(spec.getName(),
             new MapReduceWeaveRunnable(spec.getName(), "hConf.xml", "cConf.xml"),
             resourceSpec)
        .withLocalFiles()
          .add(programLocation.getName(), programLocation.toURI())
          .add("hConf.xml", hConfig.toURI())
          .add("cConf.xml", cConfig.toURI()).apply()
      .anyOrder().build();
  }
}
