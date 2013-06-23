/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Type;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveSpecification;
import com.continuuity.weave.filesystem.Location;
import com.google.common.base.Preconditions;

import java.io.File;
import java.util.Map;

/**
 *
 */
public final class FlowWeaveApplication implements WeaveApplication {

  private static final int FLOWLET_MEMORY_MB = 512;

  private final FlowSpecification spec;
  private final Program program;
  private final File hConfig;
  private final File cConfig;

  public FlowWeaveApplication(Program program, FlowSpecification spec, File hConfig, File cConfig) {
    this.spec = spec;
    this.program = program;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
  }

  @Override
  public WeaveSpecification configure() {
    WeaveSpecification.Builder.MoreRunnable moreRunnable = WeaveSpecification.Builder.with()
      .setName(String.format("%s.%s.%s.%s",
                             Type.FLOW.name(), program.getAccountId(), program.getApplicationId(), spec.getName()))
      .withRunnable();

    Location programLocation = program.getProgramJarLocation();
    String programName = programLocation.getName();
    WeaveSpecification.Builder.RunnableSetter runnableSetter = null;
    for (Map.Entry<String, FlowletDefinition> entry  : spec.getFlowlets().entrySet()) {
      ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
        .setCores(1)
        .setMemory(FLOWLET_MEMORY_MB, ResourceSpecification.SizeUnit.MEGA)  // TODO (ENG-2526): have it exposed to user
        .setInstances(entry.getValue().getInstances())
        .build();

      String flowletName = entry.getKey();
      runnableSetter = moreRunnable.add(flowletName,
                                        new FlowletWeaveRunnable(flowletName, "hConf.xml", "cConf.xml"),
                                        resourceSpec).withLocalFiles().add(programName, programLocation.toURI())
                                                                      .add("hConf.xml", hConfig.toURI())
                                                                      .add("cConf.xml", cConfig.toURI()).apply();
    }

    Preconditions.checkState(runnableSetter != null, "No flowlet for the flow.");
    return runnableSetter.anyOrder().build();
  }
}
