/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.workflow.WorkflowSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.weave.api.EventHandler;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.filesystem.Location;

import java.io.File;

/**
 *
 */
public class WorkflowWeaveApplication implements WeaveApplication {

  private static final int WORKFLOW_MEMORY_MB = 512;

  private final WorkflowSpecification spec;
  private final Program program;
  private final File hConfig;
  private final File cConfig;
  private final EventHandler eventHandler;

  public WorkflowWeaveApplication(Program program, WorkflowSpecification spec,
                                  File hConfig, File cConfig, EventHandler eventHandler) {
    this.spec = spec;
    this.program = program;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
    this.eventHandler = eventHandler;
  }

  @Override
  public WeaveSpecification configure() {
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setVirtualCores(1)
      .setMemory(WORKFLOW_MEMORY_MB, ResourceSpecification.SizeUnit.MEGA)
      .setInstances(1)
      .build();

    Location programLocation = program.getJarLocation();

    return WeaveSpecification.Builder.with()
      .setName(String.format("%s.%s.%s.%s",
                             Type.WORKFLOW.name().toLowerCase(),
                             program.getAccountId(), program.getApplicationId(), spec.getName()))
      .withRunnable()
      .add(spec.getName(),
           new WorkflowWeaveRunnable(spec.getName(), "hConf.xml", "cConf.xml"),
           resourceSpec)
      .withLocalFiles()
      .add(programLocation.getName(), programLocation.toURI())
      .add("hConf.xml", hConfig.toURI())
      .add("cConf.xml", cConfig.toURI()).apply()
      .anyOrder().withEventHandler(eventHandler).build();
  }
}
