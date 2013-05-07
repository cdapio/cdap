/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.internal.app.runtime.distributed;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.filesystem.Location;
import com.continuuity.weave.api.ResourceSpecification;
import com.continuuity.weave.api.WeaveApplication;
import com.continuuity.weave.api.WeaveSpecification;
import com.google.common.base.Preconditions;

import java.io.File;
import java.util.Map;

/**
 *
 */
public final class FlowWeaveApplication implements WeaveApplication {

  private final FlowSpecification flowSpec;
  private final Location programLocation;
  private final File hConfig;
  private final File cConfig;

  public FlowWeaveApplication(FlowSpecification flowSpec, Location programLocation, File hConfig, File cConfig) {
    this.flowSpec = flowSpec;
    this.programLocation = programLocation;
    this.hConfig = hConfig;
    this.cConfig = cConfig;
  }

  @Override
  public WeaveSpecification configure() {
    WeaveSpecification.Builder.MoreRunnable moreRunnable = WeaveSpecification.Builder.with()
      .setName(flowSpec.getName())
      .withRunnable();

    String programName = programLocation.getName();
    WeaveSpecification.Builder.RunnableSetter runnableSetter = null;
    for (Map.Entry<String, FlowletDefinition> entry  : flowSpec.getFlowlets().entrySet()) {
      ResourceSpecification resourceSpec = ResourceSpecification.Builder.with()
        .setCores(1)
        .setMemory(512, ResourceSpecification.SizeUnit.MEGA)    // TODO(terence): have it exposed to user setting
        .setInstances(entry.getValue().getInstances())
        .build();

      String flowletName = entry.getKey();
      runnableSetter = moreRunnable.add(flowletName, new FlowletWeaveRunnable(flowletName, "hConf.xml", "cConf.xml"))
                                     .withLocalFiles().add(programName, programLocation.toURI())
                                                      .add("hConf.xml", hConfig.toURI())
                                                      .add("cConf.xml", cConfig.toURI()).apply();
    }

    Preconditions.checkState(runnableSetter != null, "No flowlet for the flow.");
    return runnableSetter.anyOrder().build();
  }
}
