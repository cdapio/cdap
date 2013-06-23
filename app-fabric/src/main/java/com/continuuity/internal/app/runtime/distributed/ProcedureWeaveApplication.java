/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.procedure.ProcedureSpecification;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.filesystem.Location;

import java.io.File;

/**
 *
 */
public final class ProcedureWeaveApplication implements WeaveApplication {

  private final ProcedureSpecification spec;
  private final Program program;
  private final File hConfig;
  private final File cConfig;

  public ProcedureWeaveApplication(Program program, ProcedureSpecification spec, File hConfig, File cConfig) {
    this.spec = spec;
    this.program = program;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
  }

  @Override
  public WeaveSpecification configure() {
    ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
      .setCores(1)
      .setMemory(512, ResourceSpecification.SizeUnit.MEGA)    // TODO(ENG-2526): have it exposed to user setting
      .setInstances(1)
      .build();

    Location programLocation = program.getProgramJarLocation();

    return WeaveSpecification.Builder.with()
      .setName(String.format("%s.%s.%s.%s",
                             Type.PROCEDURE.name(), program.getAccountId(), program.getApplicationId(), spec.getName()))
      .withRunnable()
        .add(spec.getName(),
             new ProcedureWeaveRunnable(spec.getName(), "hConf.xml", "cConf.xml"),
             resourceSpec)
        .withLocalFiles()
          .add(programLocation.getName(), programLocation.toURI())
          .add("hConf.xml", hConfig.toURI())
          .add("cConf.xml", cConfig.toURI()).apply()
      .anyOrder().build();
  }
}
